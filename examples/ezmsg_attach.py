import ezmsg.core as ez

from ezmsg.util.debuglog import DebugLog

if __name__ == "__main__":
    print("This example attaches to the system created/run by ezmsg_toy.py.")
    log = DebugLog()
    ez.run(LOG=log, connections=(("GLOBAL_PING_TOPIC", log.INPUT),))
