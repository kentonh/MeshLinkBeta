import cfg
import plugins.liblogger as logger
from meshtastic import mesh_pb2, portnums_pb2

def getUserLong(interface,packet):
    ret=None
    node = getNode(interface,packet)
    if(node and "user" in node):
        ret = str(node["user"]["longName"])
        return ret

    ret = decimal_to_hex(packet["from"])
    return ret

def getUserShort(interface,packet):
    ret=None
    node = getNode(interface,packet)
    if(node and "user" in node):
        ret = str(node["user"]["shortName"])
    return ret

def getNode(interface,packet):
    ret = None
    if(packet["fromId"] in interface.nodes):
        ret = interface.nodes[packet["fromId"]]
    return ret

def decimal_to_hex(decimal_number):
    return f"!{decimal_number:08x}"

def getPosition(interface,packet):
    lat = None
    long = None
    hasPos = False
    
    node = getNode(interface,packet)
    if(packet["fromId"] in interface.nodes):
        if("position" in node):
                if("latitude" in node["position"] and "longitude" in node["position"]):
                    lat = node["position"]["latitude"]
                    long = node["position"]["longitude"]
                    hasPos = True
                     
    return lat, long, hasPos


def sendReply(text, interface, packet, channelIndex = -1, retries = 2):
    ret = packet

    if(channelIndex == -1):
        channelIndex = cfg.config["send_channel_index"]

    to = 4294967295 # ^all

    if(packet["to"] == interface.localNode.nodeNum):
         to = packet["from"]

    # Get the original packet ID to use as reply_id
    replyId = packet.get("id", 0)

    for attempt in range(retries + 1):
        try:
            sendTextWithReplyId(interface, text, to, channelIndex, replyId)
            return ret
        except Exception as e:
            if attempt < retries:
                logger.warn(f"sendReply: attempt {attempt + 1} failed, retrying: {e}")
                import time
                time.sleep(1)
            else:
                logger.warn(f"sendReply: failed after {retries + 1} attempts: {e}")

    return ret

def sendTextWithReplyId(interface, text, destinationId, channelIndex, replyId):
    """Send a text message with reply_id set to thread the message."""
    meshPacket = mesh_pb2.MeshPacket()
    meshPacket.channel = channelIndex
    meshPacket.decoded.payload = text.encode("utf-8")
    meshPacket.decoded.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
    meshPacket.decoded.reply_id = replyId
    meshPacket.id = interface._generatePacketId()

    return interface._sendPacket(meshPacket, destinationId)