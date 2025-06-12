import logging

logging.basicConfig(level=logging.INFO)

def clean_name(name):
    """Strip whitespace and capitalize the name."""
    return name.strip().title()

def is_valid_age(age):
    """Check if the age is a valid integer within range."""
    return isinstance(age, int) and 0 < age < 120

def process_user_data(users):
    """
    Takes a list of user dictionaries with 'name' and 'age',
    cleans the data, and filters out invalid users.
    """
    valid_users = []

    for user in users:
        name = clean_name(user.get("name", ""))
        age = user.get("age", None)

        if not is_valid_age(age):
            logging.warning(f"Invalid age for user: {name} ({age})")
            continue

        valid_user = {
            "name": name,
            "age": age,
            "is_adult": age >= 18
        }

        valid_users.append(valid_user)
        logging.info(f"Processed user: {valid_user}")

    return valid_users

# Example usage
if __name__ == "__main__":
    sample_users = [
        {"name": " alice ", "age": 30},
        {"name": "bob", "age": 15},
        {"name": "  charlie", "age": -5},
        {"name": "dave", "age": 130}
    ]
    print(process_user_data(sample_users))
