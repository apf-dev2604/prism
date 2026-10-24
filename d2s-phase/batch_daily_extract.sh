#!/bin/bash

SRC_PWD="1WsM06vocRC@"

START_DATE="2024-11-04"
END_DATE="2024-11-05"

CURRENT_DATE="$START_DATE"

while [[ "$CURRENT_DATE" < "$END_DATE" ]]; do
       NEXT_DATE=$(date -d "$CURRENT_DATE +1 day" +"%Y-%m-%d")

       # Do not go beyond the actual END_DATE
       if [[ "$NEXT_DATE" > "$END_DATE" ]]; then
           NEXT_DATE="$END_DATE"
       fi
      
       FROM_DATE="${CURRENT_DATE} 06:00:00"
       TO_DATE="${NEXT_DATE} 06:00:00"  

       echo "======================================================"
       echo "Running daily batch"
       echo "START_DATE : $START_DATE"
       echo "END_DATE   : $END_DATE"
       echo "TO         : $TO_DATE"
       echo "======================================================"

       #python upd_egms_games_txn_mysql_extract.py \
       python egms_gametx_inplayv1_mysql_extractor.py \
	    --table EGMS_Game_Trans \
	    --pwd "$SRC_PWD" \
	    --from "$FROM_DATE" \
	    --to "$TO_DATE" \
	    --date-column GameDate \

       PYPROC=$?

       if [ $PYPROC -ne 0 ] ; then
	   echo "FAILED: $CURRENT_DATE"
	   echo "Exit code: $PYPROC"
	   echo $PYRROC
       fi

       echo "SUCCESS: $CURRENT_DATE"
       echo 

       CURRENT_DATE="$NEXT_DATE"

done


echo "======================================================"
echo "All daily batches completed successfully."
#echo "======================================================"
