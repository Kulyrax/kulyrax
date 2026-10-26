# Renders docs/demo.gif with VHS (https://github.com/charmbracelet/vhs):
#
#   docker run --rm -v "$PWD:/vhs" ghcr.io/charmbracelet/vhs docs/demo.tape
#
# It downloads the released Linux binary first, so set VERSION to a published tag.

Output docs/demo.gif
Set Shell bash
Set FontSize 18
Set Width 1100
Set Height 620
Set Padding 24
Set TypingSpeed 45ms

Hide
Type "VERSION=v0.9.0 && cd /tmp && curl -sL https://github.com/Kulyrax/kulyrax/releases/download/$VERSION/kulyrax-$VERSION-x86_64-unknown-linux-gnu.tar.gz | tar xz && cp kulyrax-*/kulyrax /usr/local/bin/ && clear"
Enter
Sleep 4s
Show

Type "kulyrax init demo"
Enter
Sleep 2s

Type "cd demo && cat routing.yaml mappings/document/user_registered.yaml | grep -v '^#'"
Enter
Sleep 3s

Type "kulyrax run --config config.yaml --mappings mappings --once"
Enter
Sleep 3s

Type "cat out/users/u1.json"
Enter
Sleep 3s
