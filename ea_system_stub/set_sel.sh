re='^[0-9\.]+$'
if ! [[ $1 =~ $re ]] ; then
   echo "error: input '"$1"' not a number" >&2;
   echo "usage set_sel.sh <selectivity>"
   exit 1
fi
curl -X GET 'localhost:8765/set_selectivity/'$1
