import random
import string

CHARS = string.ascii_lowercase + string.digits


def random_string(length: int) -> str:
    """Create a random string of certain length.

    Args:
        length (int): The length of the random string to create.

    Returns:
        str: The random string.
    """
    return ''.join(random.choices(CHARS, k=length))

