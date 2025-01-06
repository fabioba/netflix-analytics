
# Define the task
def get_query(file_path):
    with open(file_path, "r") as file:
        return file.read()
