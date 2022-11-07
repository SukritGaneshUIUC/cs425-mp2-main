import re

INTRODUCER_HOST = "fa22-cs425-9701.cs.illinois.edu"


class Status:
    NEW = 'NEW'
    RUNNING = 'RUNNING'
    LEAVE = 'LEAVE'
class Type:
    PING = "Ping"
    PONG = "Pong"
    JOIN = "Join"

class Field:
    TYPE = "Type"
    MEMBERSHIP = "Membership"

def get_neighbors(host):
    # number = int(re.findall(r'97(.+).c', host)[0])
    # predecessor = number - 1
    # successor = number + 1
    # for i in range(3):
    #     if i < 2:
    #         if predecessor < 1:
    #             yield "fa22-cs425-97%02d.cs.illinois.edu" % (10 + predecessor)
    #         else:
    #             yield "fa22-cs425-97%02d.cs.illinois.edu" % predecessor
    #         predecessor -= 1
    #     else:
    #         if successor > 10:
    #             yield "fa22-cs425-97%02d.cs.illinois.edu" % (successor - 10)
    #         else:
    #             yield "fa22-cs425-97%02d.cs.illinois.edu" % successor
    #         successor += 1

    return ["fa22-cs425-9702.cs.illinois.edu"]


def get_all_hosts():
    # l = []
    # for i in range(1, 11):
    #     l.append("fa22-cs425-97%02d.cs.illinois.edu" % i)
    # return l
    return ["fa22-cs425-9701.cs.illinois.edu", "fa22-cs425-9702.cs.illinois.edu"]



