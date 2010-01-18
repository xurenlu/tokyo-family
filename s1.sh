#!/bin/bash
dir=`pwd`
port=11211
sid=11211
dist/bin/ttserver  -port $port -pid $dir/tt${sid}/ttserver.pid -log $dir/tt${sid}/ttserver.log -ld -ulog $dir/tt${sid}/ -ulim 128m -sid $sid  -rts $dir/tt/ttserver.rts $dir/tt/database.tch
