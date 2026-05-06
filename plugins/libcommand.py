import plugins.liblogger as logger
import plugins.libdiscordutil as DiscordUtil
import plugins.libinfo as LibInfo
import plugins.libmesh as LibMesh
import cfg

commands = []


def _getLocalShortName(interface):
    """Return this collector's local-node shortName, or None if unavailable."""
    try:
        info = interface.getMyNodeInfo()
        if info and "user" in info:
            short = info["user"].get("shortName")
            if short:
                return str(short)
    except Exception as e:
        logger.warn(f"libcommand: could not read local shortName: {e}")
    return None


def _parseInvocation(text, interface):
    """Parse a text message into (command_name, args, is_mention).

    Recognizes two forms:
      1. Prefix:  "<prefix>cmd [args]"           e.g. "$ping"
      2. Mention: "@<shortname> cmd [args]"      e.g. "@fl0v ping"

    For the mention form the command is only accepted when <shortname>
    matches this collector's local-node shortName (case-insensitive),
    so other collectors on the mesh stay silent.

    Returns (None, "", False) when the text is not a command for us.
    """
    if not text:
        return None, "", False

    if text.startswith("@"):
        parts = text[1:].split(maxsplit=2)
        if len(parts) >= 2:
            target = parts[0]
            local_short = _getLocalShortName(interface)
            if local_short and target.lower() == local_short.lower():
                return parts[1], (parts[2] if len(parts) > 2 else ""), True
        return None, "", False

    prefix = cfg.config["prefix"]
    if text.startswith(prefix):
        noprefix = text[len(prefix):]
        parts = noprefix.split(maxsplit=1)
        if parts:
            return parts[0], (parts[1] if len(parts) > 1 else ""), False

    return None, "", False


class simpleCommand():
    """Basic command class, use this to create simple commands.
    registerCommand("Hello", "This command tells you hello!", callback function)
    """
    name = ""
    info = ""
    callback = None


    def registerCommand(self, name, info, callback):
        self.name = name
        self.info = info
        self.callback = callback
        commands.append(self)

        LibInfo.info.append(f"{name} - {info}")

    def onReceive(self, packet, interface, client):
        if "decoded" not in packet:
            return
        if packet["decoded"].get("portnum") != "TEXT_MESSAGE_APP":
            return

        text = packet["decoded"].get("text", "")
        command_name, args, is_mention = _parseInvocation(text, interface)
        if command_name is None or command_name != self.name:
            return

        # Mentions explicitly target this collector, so they bypass the
        # ignored-channel filter that suppresses prefix commands on noisy channels.
        if not is_mention:
            send_channel = int(packet["channel"]) if "channel" in packet else 0
            ignored_channels = cfg.config.get("ignored_channel_indices", [1, 2, 3, 4, 5, 6, 7])
            if send_channel in ignored_channels:
                logger.info(f"Skipping command '{command_name}' from channel {send_channel} (ignored)")
                return

        reply = self.executeCommand(packet, interface, client, args)
        LibMesh.sendReply(reply, interface, packet)

        if cfg.config["send_mesh_commands_to_discord"]:
            bot_name = cfg.config.get("bot_name", "MeshLink")
            DiscordUtil.send_embed(bot_name + " reply", reply, client, cfg.config, color=0x6bc19b)
                    
    
    def executeCommand(self, packet, interface, client, args):
        return self.callback(packet, interface, client, args)
        

