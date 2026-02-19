import cfg
import plugins.liblogger as logger
from meshtastic import mesh_pb2, portnums_pb2
import random
import time

# Built-in defaults for response delay - works without any config
RESPONSE_DELAY_DEFAULTS = {
    'enabled': True,
    'hop_delay_seconds': 0.25,       # 100ms per hop
    'slot_count': 8,
    'slot_duration_seconds': 0.25,  # 250ms per slot
    'jitter_max_seconds': 0.1
}

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

def calculate_response_delay(interface, packet):
    """Calculate delay to prevent multi-node response collisions.

    Uses a combined strategy:
    1. Hop delay: Closer nodes respond first (100ms per hop)
    2. Node slot: Deterministic spread based on node ID hash
    3. Jitter: Random delay to break ties
    """
    # Merge config overrides with defaults
    defaults = RESPONSE_DELAY_DEFAULTS.copy()
    config_overrides = cfg.config.get('response_delay', {})
    delay_config = {**defaults, **config_overrides}

    if not delay_config.get('enabled', True):
        return 0

    # Component 1: Hop-based delay (100ms per hop by default)
    hop_start = packet.get('hopStart', 0)
    hop_limit = packet.get('hopLimit', 0)
    hops_away = (hop_start - hop_limit) if hop_start and hop_limit else 0
    hop_delay = hops_away * delay_config['hop_delay_seconds']

    # Component 2: Node ID hash slot (deterministic per-node offset)
    node_id = interface.localNode.nodeNum
    slot_count = delay_config['slot_count']
    slot_duration = delay_config['slot_duration_seconds']
    slot = hash(node_id) % slot_count
    slot_delay = slot * slot_duration

    # Component 3: Random jitter (breaks ties)
    jitter_max = delay_config['jitter_max_seconds']
    jitter = random.uniform(0, jitter_max)

    return hop_delay + slot_delay + jitter

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

    # Calculate and apply response delay to prevent collisions
    delay = calculate_response_delay(interface, packet)
    if delay > 0:
        hops_away = (packet.get('hopStart', 0) - packet.get('hopLimit', 0))
        logger.info(f"Response delay: {delay:.2f}s (hops={hops_away})")
        time.sleep(delay)

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