from sql import *
from sqltools import (dbInit, dbCommit)

dbInit()
resetbalances_MP()
dbCommit()

