#!/bin/bash

set -o nounset

set -o pipefail

set -o errexit

shopt -s failglob

if [[ $# -lt 1 ]] ; then
    echo "Usage: $0 <directory>" >&2
    exit 1
fi

DIRNAME="$1"

if [ ! -d "$DIRNAME" ]; then
    echo "Not a directory: $DIRNAME" >&2
    exit 1;
fi

if [ ! -x "$DIRNAME" ]; then
    echo "Executable permission missing: $DIRNAME" >&2
    exit 1;
fi

if [ ! -r "$DIRNAME" ]; then
    echo "Read permission missing: $DIRNAME" >&2
    exit 1;
fi

FILENAME="$DIRNAME/index.html"

cat > $FILENAME <<EOF
<html>
  <head>
    <title>${DIRNAME}</title>
    <style type="text/css">
* {
  font-family: sans-serif;
}
    </style>
  </head>
  <body>
    <h1>Install Counts by Plugin Version and Jenkins Version</h1>

    <h2>Plugins</h2>
    <ul>
EOF

for f in "$DIRNAME"/*.html ; do
    FILE=$(basename $f)
    cat >> $FILENAME <<EOF
     <li><a href="${FILE}">${FILE/.html/}</a></li>
EOF

  done;

cat >> $FILENAME <<EOF

    </ul>
    <br/>
    <center>
        This page generated by
        <a href="https://github.com/jenkins-infra/infra-statistics/blob/master/$(basename $0)">this script</a>
    </center>
  </body>
</html>
EOF