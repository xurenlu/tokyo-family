#!/bin/bash
dir=`pwd`
port=11212
sid=11212
dist/bin/ttserver  -port $port -pid $dir/tt${sid}/ttserver.pid -log $dir/tt${sid}/ttserver.log -ld -ulog $dir/tt${sid}/ -ulim 128m -sid $sid -mhost 127.0.0.1 -mport 11211 -rts $dir/tt${sid}/ttserver.rts $dir/tt${sid}/database.tch
