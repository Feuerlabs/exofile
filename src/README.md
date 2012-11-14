
exofile file transfer
=====================

The exofile is a file transfer utility that may be used in various ways.
It can be used as a regular file down load utility.
But it also handles binary editing operation and can thus be used
to update bits and pieces of files.

Each file transfer starts with creating a transfer file,
that is identified by a 6 digit number.
This file keep track on all data transfered so far.

File format
===========
Meta
{ meta,  <file-size>, <chunk-size>,<type>,<name>,<mode>}
{ crc,   <crc32-for-record> }
{ write, <offset>, <binary-data> }
{ crc,   <crc32-for-record> }
{ insert, <offset>, <binary-data> }
{ crc,   <crc32-for-record> }
{ delete, <offset>, <size> }
{ crc,   <crc32-for-record> }
{ fill,  <offset>, <size>, <binary-data> }
{ crc,   <crc32-for-record> }

The chunk-size is the recommended chunk-size to use when sending
write and insert operations.
The chunk-size is negotiated with the server hinting a chunk size
that the server thinks is appropriate, the client may the change
that reccomendation in the operation reply.

In memory a file region map is kept to keep track on what
portions are defined.
