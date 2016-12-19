"""
Classes and functions for checking GPG signatures against arbitrary payloads.
"""

import eva
import eva.globe

import dateutil.parser
import os
import re
import subprocess
import tempfile


class GPGSignatureCheckResult(object):
    def __init__(self, exit_code, stdout, stderr):
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.timestamp = None
        self.key_type = None
        self.key_id = None
        self.signer = None
        self.parse_stderr()

    def parse_stderr(self):
        re_signature = re.compile('^gpg: Signature made (.+) using (.+) key ID ([\w\d]+)$')
        re_signer = re.compile('^gpg: Good signature from "(.+)"$')
        for line in self.stderr:
            matches = re_signature.match(line)
            if matches:
                self.timestamp = dateutil.parser.parse(matches.group(1))
                self.key_type = matches.group(2)
                self.key_id = matches.group(3)
            matches = re_signer.match(line)
            if matches:
                self.signer = matches.group(1)


class GPGSignatureChecker(eva.globe.GlobalMixin):
    def __init__(self, payload, signature):
        self.payload = payload
        self.signature = signature
        self.directory = tempfile.TemporaryDirectory()

    def write_temporary_files(self):
        self.payload_file = os.path.join(self.directory.name, 'request')
        self.signature_file = os.path.join(self.directory.name, 'request.asc')
        with open(self.payload_file, 'wb') as f:
            f.write(self.payload.encode('utf-8'))
        with open(self.signature_file, 'wb') as f:
            for line in self.signature:
                data = '%s\n' % line
                f.write(data.encode('utf-8'))

    def verify(self):
        self.write_temporary_files()
        cmd = ['gpg', '--verify', self.signature_file]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = proc.communicate()
        result = GPGSignatureCheckResult(
            proc.returncode,
            stdout.strip().splitlines(),
            stderr.strip().splitlines(),
        )
        return result
