from omnidex import getOrderbook
from sqltools import *
from cacher import *
import json
import time
import datetime
import sys


def printmsg(msg):
    print str(datetime.datetime.now()) + str(" ") + str(msg)
    sys.stdout.flush()


def updateOrderbookCache():
    while True:
        time.sleep(20)
        printmsg("Checking for orderbook updates")
        try:
            lasttrade = rGet("omniwallet:omnidex:lasttrade")
            if lasttrade is None:
                lasttrade = 0

            lastpending = rGet("omniwallet:omnidex:lastpending")
            if lastpending is None:
                lastpending = 0

            ret = getOrderbook(lasttrade, lastpending)
            if ret['updated']:
                printmsg("Orderbook cache updated. Lasttrade: " + str(lasttrade) + " Lastpending: " + str(lastpending))
                rSet("omniwallet:omnidex:lasttrade", ret['lasttrade'])
                rSet("omniwallet:omnidex:lastpending", ret['lastpending'])
                rSet("omniwallet:omnidex:book", json.dumps(ret['book']))
        except Exception as e:
            printmsg("Error updating orderbook cache " + str(e))
        dbCommit()


def main():
    updateOrderbookCache()


if __name__ == "__main__": main()  ## with if
