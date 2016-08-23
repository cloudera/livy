# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import ast
import datetime
import decimal
import io
import json
import logging
import sys
import traceback
import base64
import os
import re
import threading
import tempfile
import shutil
import pickle

if sys.version >= '3':
    unicode = str
else:
    import cStringIO
    import StringIO

logging.basicConfig()
LOG = logging.getLogger('fake_shell')

global_dict = {}
job_context = None
local_tmp_dir_path = None

TOP_FRAME_REGEX = re.compile(r'\s*File "<stdin>".*in <module>')

def execute_reply(status, content):
    return {
        'msg_type': 'execute_reply',
        'content': dict(
            content,
            status=status,
        )
    }


def execute_reply_ok(data):
    return execute_reply('ok', {
        'data': data,
    })


def execute_reply_error(exc_type, exc_value, tb):
    LOG.error('execute_reply', exc_info=True)
    if sys.version >= '3':
      formatted_tb = traceback.format_exception(exc_type, exc_value, tb, chain=False)
    else:
      formatted_tb = traceback.format_exception(exc_type, exc_value, tb)
    for i in range(len(formatted_tb)):
        if TOP_FRAME_REGEX.match(formatted_tb[i]):
            formatted_tb = formatted_tb[:1] + formatted_tb[i + 1:]
            break

    return execute_reply('error', {
        'ename': unicode(exc_type.__name__),
        'evalue': unicode(exc_value),
        'traceback': formatted_tb,
    })


def execute_reply_internal_error(message, exc_info=None):
    LOG.error('execute_reply_internal_error', exc_info=exc_info)
    return execute_reply('error', {
        'ename': 'InternalError',
        'evalue': message,
        'traceback': [],
    })


class JobContextImpl(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.sc = global_dict['sc']
        self.sql_ctx = global_dict['sqlContext']
        self.hive_ctx = None
        self.streaming_ctx = None
        self.local_tmp_dir_path = local_tmp_dir_path

    def sc(self):
        return self.sc

    def sql_ctx(self):
        return self.sql_ctx

    def hive_ctx(self):
        if self.hive_ctx is None:
            with self.lock:
                if self.hive_ctx is None:
                    if isinstance(self.sql_ctx, global_dict['HiveContext']):
                        self.hive_ctx = self.sql_ctx
                    else:
                        self.hive_ctx = global_dict['HiveContext'](self.sc)
        return self.hive_ctx

    def create_streaming_ctx(self, batch_duration):
        with self.lock:
            if self.streaming_ctx is not None:
                raise ValueError("Streaming context already exists")
            self.streaming_ctx = global_dict['StreamingContext'](self.sc, batch_duration)

    def streaming_ctx(self):
        with self.lock:
            if self.streaming_ctx is None:
                raise ValueError("create_streaming_ctx function should be called first")
        return self.streaming_ctx

    def stop_streaming_ctx(self):
        with self.lock:
            if self.streaming_ctx is None:
                raise ValueError("Cannot stop streaming context. Streaming context is None")
            self.streaming_ctx.stop()
            self.streaming_ctx = None

    def get_local_tmp_dir_path(self):
        return self.local_tmp_dir_path

    def stop(self):
        with self.lock:
            if self.streaming_ctx is not None:
                self.stop_streaming_ctx()
            if self.sc is not None:
                self.sc.stop()


class PySparkJobProcessorImpl(object):
    def processBypassJob(self, serialized_job):
        try:
            if sys.version >= '3':
                deserialized_job = pickle.loads(serialized_job, encoding="bytes")
            else:
                deserialized_job = pickle.loads(serialized_job)
            result = deserialized_job(job_context)
            serialized_result = global_dict['cloudpickle'].dumps(result)
            response = bytearray(base64.b64encode(serialized_result))
        except:
            response = bytearray('Error job(' + traceback.format_exc() + ')', 'utf-8')
        return response

    def addFile(self, uri_path):
        job_context.sc.addFile(uri_path)

    def addPyFile(self, uri_path):
        job_context.sc.addPyFile(uri_path)

    def getLocalTmpDirPath(self):
        return os.path.join(job_context.get_local_tmp_dir_path(), '__livy__')

    class Scala:
        extends = ['com.cloudera.livy.repl.PySparkJobProcessor']


class ExecutionError(Exception):
    def __init__(self, exc_info):
        self.exc_info = exc_info


class NormalNode(object):
    def __init__(self, code):
        self.code = compile(code, '<stdin>', 'exec', ast.PyCF_ONLY_AST, 1)

    def execute(self):
        to_run_exec, to_run_single = self.code.body[:-1], self.code.body[-1:]

        try:
            for node in to_run_exec:
                mod = ast.Module([node])
                code = compile(mod, '<stdin>', 'exec')
                exec(code, global_dict)

            for node in to_run_single:
                mod = ast.Interactive([node])
                code = compile(mod, '<stdin>', 'single')
                exec(code, global_dict)
        except:
            # We don't need to log the exception because we're just executing user
            # code and passing the error along.
            raise ExecutionError(sys.exc_info())


class UnknownMagic(Exception):
    pass


class MagicNode(object):
    def __init__(self, line):
        parts = line[1:].split(' ', 1)
        if len(parts) == 1:
            self.magic, self.rest = parts[0], ()
        else:
            self.magic, self.rest = parts[0], (parts[1],)

    def execute(self):
        if not self.magic:
            raise UnknownMagic('magic command not specified')

        try:
            handler = magic_router[self.magic]
        except KeyError:
            raise UnknownMagic("unknown magic command '%s'" % self.magic)

        return handler(*self.rest)


def parse_code_into_nodes(code):
    nodes = []
    try:
        nodes.append(NormalNode(code))
    except SyntaxError:
        # It's possible we hit a syntax error because of a magic command. Split the code groups
        # of 'normal code', and code that starts with a '%'. possibly magic code
        # lines, and see if any of the lines
        # Remove lines until we find a node that parses, then check if the next line is a magic
        # line
        # .

        # Split the code into chunks of normal code, and possibly magic code, which starts with
        # a '%'.

        normal = []
        chunks = []
        for i, line in enumerate(code.rstrip().split('\n')):
            if line.startswith('%'):
                if normal:
                    chunks.append('\n'.join(normal))
                    normal = []

                chunks.append(line)
            else:
                normal.append(line)

        if normal:
            chunks.append('\n'.join(normal))

        # Convert the chunks into AST nodes. Let exceptions propagate.
        for chunk in chunks:
            if chunk.startswith('%'):
                nodes.append(MagicNode(chunk))
            else:
                nodes.append(NormalNode(chunk))

    return nodes


def execute_request(content):
    try:
        code = content['code']
    except KeyError:
        return execute_reply_internal_error(
            'Malformed message: content object missing "code"', sys.exc_info()
        )

    try:
        nodes = parse_code_into_nodes(code)
    except SyntaxError:
        exc_type, exc_value, tb = sys.exc_info()
        return execute_reply_error(exc_type, exc_value, None)

    result = None

    try:
        for node in nodes:
            result = node.execute()
    except UnknownMagic:
        exc_type, exc_value, tb = sys.exc_info()
        return execute_reply_error(exc_type, exc_value, None)
    except ExecutionError as e:
        return execute_reply_error(*e.exc_info)

    if result is None:
        result = {}

    stdout = sys.stdout.getvalue()
    stderr = sys.stderr.getvalue()

    clearOutputs()

    output = result.pop('text/plain', '')

    if stdout:
        output += stdout

    if stderr:
        output += stderr

    output = output.rstrip()

    # Only add the output if it exists, or if there are no other mimetypes in the result.
    if output or not result:
        result['text/plain'] = output.rstrip()

    return execute_reply_ok(result)


def magic_table_convert(value):
    try:
        converter = magic_table_types[type(value)]
    except KeyError:
        converter = magic_table_types[str]

    return converter(value)


def magic_table_convert_seq(items):
    last_item_type = None
    converted_items = []

    for item in items:
        item_type, item = magic_table_convert(item)

        if last_item_type is None:
            last_item_type = item_type
        elif last_item_type != item_type:
            raise ValueError('value has inconsistent types')

        converted_items.append(item)

    return 'ARRAY_TYPE', converted_items


def magic_table_convert_map(m):
    last_key_type = None
    last_value_type = None
    converted_items = {}

    for key, value in m:
        key_type, key = magic_table_convert(key)
        value_type, value = magic_table_convert(value)

        if last_key_type is None:
            last_key_type = key_type
        elif last_value_type != value_type:
            raise ValueError('value has inconsistent types')

        if last_value_type is None:
            last_value_type = value_type
        elif last_value_type != value_type:
            raise ValueError('value has inconsistent types')

        converted_items[key] = value

    return 'MAP_TYPE', converted_items


magic_table_types = {
    type(None): lambda x: ('NULL_TYPE', x),
    bool: lambda x: ('BOOLEAN_TYPE', x),
    int: lambda x: ('INT_TYPE', x),
    float: lambda x: ('DOUBLE_TYPE', x),
    str: lambda x: ('STRING_TYPE', str(x)),
    datetime.date: lambda x: ('DATE_TYPE', str(x)),
    datetime.datetime: lambda x: ('TIMESTAMP_TYPE', str(x)),
    decimal.Decimal: lambda x: ('DECIMAL_TYPE', str(x)),
    tuple: magic_table_convert_seq,
    list: magic_table_convert_seq,
    dict: magic_table_convert_map,
}

# python 2.x only
if sys.version < '3':
    magic_table_types.update({
        long: lambda x: ('BIGINT_TYPE', x),
        unicode: lambda x: ('STRING_TYPE', x.encode('utf-8'))
    })



def magic_table(name):
    try:
        value = global_dict[name]
    except KeyError:
        exc_type, exc_value, tb = sys.exc_info()
        return execute_reply_error(exc_type, exc_value, None)

    if not isinstance(value, (list, tuple)):
        value = [value]

    headers = {}
    data = []

    for row in value:
        cols = []
        data.append(cols)

        if not isinstance(row, (list, tuple, dict)):
            row = [row]

        if isinstance(row, (list, tuple)):
            iterator = enumerate(row)
        else:
            iterator = sorted(row.items())

        for name, col in iterator:
            col_type, col = magic_table_convert(col)

            try:
                header = headers[name]
            except KeyError:
                header = {
                    'name': str(name),
                    'type': col_type,
                }
                headers[name] = header
            else:
                # Reject columns that have a different type.
                if header['type'] != col_type:
                    exc_type = Exception
                    exc_value = 'table rows have different types'
                    return execute_reply_error(exc_type, exc_value, None)

            cols.append(col)

    headers = [v for k, v in sorted(headers.items())]

    return {
        'application/vnd.livy.table.v1+json': {
            'headers': headers,
            'data': data,
        }
    }


def magic_json(name):
    try:
        value = global_dict[name]
    except KeyError:
        exc_type, exc_value, tb = sys.exc_info()
        return execute_reply_error(exc_type, exc_value, None)

    return {
        'application/json': value,
    }

def magic_matplot(name):
    try:
        value = global_dict[name]
        fig = value.gcf()
        imgdata = io.BytesIO()
        fig.savefig(imgdata, format='png')
        imgdata.seek(0)
        encode = base64.b64encode(imgdata.getvalue())
        if sys.version >= '3':
            encode = encode.decode()

    except:
        exc_type, exc_value, tb = sys.exc_info()
        return execute_reply_error(exc_type, exc_value, None)

    return {
        'image/png': encode,
        'text/plain': "",
    }

def shutdown_request(_content):
    sys.exit()


magic_router = {
    'table': magic_table,
    'json': magic_json,
    'matplot': magic_matplot,
}

msg_type_router = {
    'execute_request': execute_request,
    'shutdown_request': shutdown_request,
}

class UnicodeDecodingStringIO(io.StringIO):
    def write(self, s):
        if isinstance(s, bytes):
            s = s.decode("utf-8")
        super(UnicodeDecodingStringIO, self).write(s)


def clearOutputs():
    sys.stdout.close()
    sys.stderr.close()
    sys.stdout = UnicodeDecodingStringIO()
    sys.stderr = UnicodeDecodingStringIO()


def main():
    sys_stdin = sys.stdin
    sys_stdout = sys.stdout
    sys_stderr = sys.stderr

    if sys.version >= '3':
        sys.stdin = io.StringIO()
    else:
        sys.stdin = cStringIO.StringIO()

    sys.stdout = UnicodeDecodingStringIO()
    sys.stderr = UnicodeDecodingStringIO()

    try:
        listening_port = 0
        if os.environ.get("LIVY_TEST") != "true":
            #Load spark into the context
            exec('from pyspark.shell import sc', global_dict)
            exec('from pyspark.shell import sqlContext', global_dict)
            exec('from pyspark.sql import HiveContext', global_dict)
            exec('from pyspark.streaming import StreamingContext', global_dict)
            exec('import pyspark.cloudpickle as cloudpickle', global_dict)

            #Start py4j callback server
            from py4j.protocol import ENTRY_POINT_OBJECT_ID
            from py4j.java_gateway import JavaGateway, GatewayClient, CallbackServerParameters

            gateway_client_port = int(os.environ.get("PYSPARK_GATEWAY_PORT"))
            gateway = JavaGateway(GatewayClient(port=gateway_client_port))
            gateway.start_callback_server(
                callback_server_parameters=CallbackServerParameters(port=0))
            socket_info = gateway._callback_server.server_socket.getsockname()
            listening_port = socket_info[1]
            pyspark_job_processor = PySparkJobProcessorImpl()
            gateway.gateway_property.pool.dict[ENTRY_POINT_OBJECT_ID] = pyspark_job_processor

            global local_tmp_dir_path, job_context
            local_tmp_dir_path = tempfile.mkdtemp()
            job_context = JobContextImpl()

        print(sys.stdout.getvalue(), file=sys_stderr)
        print(sys.stderr.getvalue(), file=sys_stderr)

        clearOutputs()

        print('READY(port=' + str(listening_port) + ')', file=sys_stdout)
        sys_stdout.flush()

        while True:
            line = sys_stdin.readline()

            if line == '':
                break
            elif line == '\n':
                continue

            try:
                msg = json.loads(line)
            except ValueError:
                LOG.error('failed to parse message', exc_info=True)
                continue

            try:
                msg_type = msg['msg_type']
            except KeyError:
                LOG.error('missing message type', exc_info=True)
                continue

            try:
                content = msg['content']
            except KeyError:
                LOG.error('missing content', exc_info=True)
                continue

            if not isinstance(content, dict):
                LOG.error('content is not a dictionary')
                continue

            try:
                handler = msg_type_router[msg_type]
            except KeyError:
                LOG.error('unknown message type: %s', msg_type)
                continue

            response = handler(content)

            try:
                response = json.dumps(response)
            except ValueError:
                response = json.dumps({
                    'msg_type': 'inspect_reply',
                    'content': {
                        'status': 'error',
                        'ename': 'ValueError',
                        'evalue': 'cannot json-ify %s' % response,
                        'traceback': [],
                    }
                })

            print(response, file=sys_stdout)
            sys_stdout.flush()
    finally:
        if os.environ.get("LIVY_TEST") != "true" and 'sc' in global_dict:
            gateway.shutdown_callback_server()
            shutil.rmtree(local_tmp_dir_path)
            global_dict['sc'].stop()

        sys.stdin = sys_stdin
        sys.stdout = sys_stdout
        sys.stderr = sys_stderr

if __name__ == '__main__':
    sys.exit(main())
