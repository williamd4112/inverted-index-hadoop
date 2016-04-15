echo "Retrievaling..."
sh retrieval.sh "$1" > null
echo "Result sorting..."
sh result.sh > null
cat sorted_result/*

