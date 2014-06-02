Distributed-Database
====================

A distributed database engine implemented by Java RMI, support similarity search (edit_distance and jaccard)

The system fragment table to 4 servers. 

This is a example:

Publisher.1            id>=104000 AND nation=’USA’

Publisher.2            id<104000 AND nation=’RC’

Publisher.3            ￼￼id<104000 AND nation=’USA’

Publisher.4            id>=104000 AND nation=’USA’
