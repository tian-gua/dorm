import random
import string


def generate_random_string(prefix: str = "", length=10, seed: int | None = None) -> str:
    """
    生成一个随机的字符串，包含字母和数字。

    Args:
        prefix (str): 字符串前缀，默认为空字符串。
        length (int): 字符串长度，默认为10。
        seed (int | None): 随机种子，默认为 None。

    Returns:
        str: 随机生成的字符串。
    """
    if seed is not None:
        random.seed(seed)
    characters = string.ascii_letters + string.digits
    return prefix + "".join(random.choices(characters, k=length))
