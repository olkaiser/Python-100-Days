#!/bin/bash
# Filename: call_data.sh

# Input parameters
SCRIPT_TO_CALL=$1
DATE_START=$2
DATE_END=$3

# Function to calculate the last day of the month
function last_day() {
    YEAR=${1:0:4}
    MONTH=${1:4:2}
    LAST_DAY=$(cal $MONTH $YEAR | awk 'NF {DAYS = $NF}; END {print DAYS}')
    echo $LAST_DAY
}

# Calculate the month difference between DATE_START and DATE_END
DIFF_MONTHS=$(( ( ${DATE_END:0:4} - ${DATE_START:0:4} ) * 12 + ${DATE_END:4:2} - ${DATE_START:4:2} + 1 ))
echo "Month difference: $DIFF_MONTHS"

# Call the script and count the number of successful calls
SUCCESS_COUNT=0
for ((i=0; i<DIFF_MONTHS; i++)); do
    RESULT_LAST_LINE=$($SCRIPT_TO_CALL | tee /dev/fd/2 | tail -n1)
    CURRENT_MONTH=$(date -d "${DATE_START:0:4}-${DATE_START:4:2}-01 +$i month" "+%Y%m")
    LAST_DAY=$(last_day $CURRENT_MONTH)
    if [[ $RESULT_LAST_LINE == "我完事了" ]]; then
        ((SUCCESS_COUNT++))
        echo "成功调用${SUCCESS_COUNT}次"
    else
        echo "#################我没了###################"
        exit 1
    fi
    echo "${CURRENT_MONTH} ${CURRENT_MONTH}${LAST_DAY}"
done

