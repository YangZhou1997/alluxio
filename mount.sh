# /bin/bash

alluxio fs mount --option speedup.ufs.host=node1 --option speedup.ufs.port=19998 \
    --option speedup.ufs.base=/remote /remote speedup://node1:19998/