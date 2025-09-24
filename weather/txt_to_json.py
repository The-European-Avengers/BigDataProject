import json

input_file = "2024-01-01.txt"
output_file = "2024-01-01.json"

# Read lines (each is a JSON object)
with open(input_file, "r") as f:
    objects = [json.loads(line) for line in f if line.strip()]

# Write as proper JSON array
with open(output_file, "w") as f:
    json.dump(objects, f, indent=2)
