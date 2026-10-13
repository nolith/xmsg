# Oracle 2: the typed `peers` option renders what nix-config's renderer renders today
# (nix-config 174eb64c, modules/den/aspects/applications/dev/ai/mcp/xmsg.nix:352-397), for
# an equivalent input, in the CA form and in the pin form. Compared after `jq -S`.
{ inputs, ... }:
{
  perSystem =
    { pkgs, lib, ... }:
    let
      inherit (inputs) xmsg home-manager;
      crt = ./fixtures/peer.crt;
      caCrt =
        c:
        builtins.path {
          path = c;
          name = builtins.unsafeDiscardStringContext (baseNameOf c);
        };

      # The module's rendering: the `--peers-file` argument of the evaluated unit.
      modulePeers =
        peers:
        let
          c =
            (home-manager.lib.homeManagerConfiguration {
              inherit pkgs;
              modules = [
                xmsg.homeManagerModules.xmsg
                {
                  home = {
                    username = "alice";
                    homeDirectory = "/home/alice";
                    stateVersion = "25.11";
                    enableNixpkgsReleaseCheck = false;
                  };
                  programs.xmsg = {
                    enable = true;
                    package = pkgs.hello;
                    server.federation = {
                      enable = true;
                      cert = "/run/secrets/xmsg.crt";
                      key = "/run/secrets/xmsg.key";
                      inherit peers;
                    };
                  };
                }
              ];
            }).config;
          argv = lib.splitString " " (lib.head c.systemd.user.services.xmsg.Service.ExecStart);
        in
        lib.elemAt argv (lib.lists.findFirstIndex (a: a == "--peers-file") null argv + 1);

      # nix-config's renderer, verbatim but for its owner bindings (host names, the
      # spiffe scheme, the domain), which are the fixture's here.
      peerNode = "cortex";
      address = "cortex.ts.example.org:7788";
      spiffe = "spiffe://example.org/host/cortex/user/alice";
      leaves = [
        {
          name = "genie";
          identity = "spiffe://example.org/cluster/axon/genie";
          allow = [ "reply" ];
          targets = [ "genie-guard" ];
        }
      ];
      refCa = pkgs.writeText "xmsg-peers.json" (
        builtins.toJSON (
          {
            ${peerNode} = {
              inherit address;
              ca = "${caCrt crt}";
              identities = [ spiffe ];
              allow = [
                "send"
                "reply"
                "list"
              ];
            };
          }
          // lib.listToAttrs (
            map (
              leaf:
              lib.nameValuePair leaf.name {
                address = "${leaf.name}.invalid:7788";
                ca = "${caCrt crt}";
                identities = [ leaf.identity ];
                leaf = true;
                inherit (leaf) allow targets;
              }
            ) leaves
          )
        )
      );
      refPin =
        pkgs.runCommand "xmsg-peers.json"
          {
            nativeBuildInputs = [
              pkgs.jq
              pkgs.openssl
            ];
          }
          ''
            openssl x509 -in ${crt} -pubkey -noout > pub.pem
            openssl pkey -pubin -in pub.pem -outform DER > spki.der
            pin=$(sha256sum spki.der | cut -d' ' -f1)
            jq -n --arg name ${peerNode} --arg pin "sha256:$pin" \
              --arg address ${address} \
              '{($name): {address: $address, pin: $pin, allow: ["send", "reply", "list"]}}' > $out
          '';

      allowAll = [
        "send"
        "reply"
        "list"
      ];
      mineCa = modulePeers {
        cortex = {
          inherit address;
          ca = crt;
          identities = [ spiffe ];
          allow = allowAll;
        };
        genie = {
          address = "genie.invalid:7788";
          ca = crt;
          identities = [ "spiffe://example.org/cluster/axon/genie" ];
          leaf = true;
          allow = [ "reply" ];
          targets = [ "genie-guard" ];
        };
      };
      minePin = modulePeers {
        cortex = {
          inherit address;
          pinFromCert = crt;
          allow = allowAll;
        };
      };
    in
    {
      checks.peers-render =
        pkgs.runCommand "xmsg-peers-render"
          {
            nativeBuildInputs = [
              pkgs.jq
              pkgs.diffutils
            ];
          }
          ''
            same() {
              jq -S . "$2" > mine.json
              jq -S . "$3" > ref.json
              if ! diff -u ref.json mine.json; then
                echo "peers-render: the $1 form differs from nix-config's renderer (- nix-config, + module)" >&2
                exit 1
              fi
              echo "peers-render: $1 form equal"
            }
            same CA ${mineCa} ${refCa}
            same pin ${minePin} ${refPin}
            touch $out
          '';
    };
}
