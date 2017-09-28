KINESYSLOG
==========

Syslog relay to AWS Kinesis Firehose. Supports UDP, TCP, and TLS: RFC2164, RFC5424, RFC5425, RFC6587.

Prerequisites
-------------

This module requires Python 3.5 or better, due to its use of the ``asyncio`` framework.

This module uses Boto3 to make API calls against the Kinesis Firehose service. You
should have a working AWS API environment (~/.aws/credentials,
environment variables, or EC2 IAM Role) that allows calling Kinesis Firehose's
``put-record-batch`` method against the account that it is running in, and the stream
specified on the command line.
