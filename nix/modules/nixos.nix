# nixosModules.xmsg: the one root-only part of xmsg, the federation port in the firewall.
# Everything else is per user (homeManagerModules.xmsg).
{ config, lib, ... }:
let
  cfg = config.programs.xmsg.federation;
in
{
  options.programs.xmsg.federation = {
    enable = lib.mkEnableOption "the xmsg federation port in the firewall";
    port = lib.mkOption {
      type = lib.types.port;
      default = 7788;
      description = "The federation port to open; match `programs.xmsg.server.federation.port` in home-manager.";
    };
  };

  config = lib.mkIf cfg.enable {
    networking.firewall.allowedTCPPorts = [ cfg.port ];
  };
}
