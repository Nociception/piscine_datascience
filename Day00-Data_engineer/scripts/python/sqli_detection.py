import re


def sqli_detection(arg: str) -> None:
    """DOCSTRING"""

    if arg is None:
        return

    SQLI_PATTERNS = [
        r"(--|\#|/\*.*\*/)",
        r"(\bUNION\b.*\bSELECT\b)",
        r"(\bSELECT\b.*\bFROM\b.*\bWHERE\b.*)",
        r"(\bDROP\b\s+\bTABLE\b|\bDROP\b\s+\bDATABASE\b)",
        r"(\bINSERT\b\s+\bINTO\b|\bUPDATE\b\s+\bSET\b)",
        r"(\bDELETE\b\s+\bFROM\b)",
        r"(\bOR\b\s+\d+=\d+|\bAND\b\s+\d+=\d+)",
        r"(';|--|#|/\*|\*/|`|\"|\$)",
        r"(\bpg_sleep\b|\bxp_cmdshell\b|\bbenchmark\b)",
    ]

    SQLI_REGEX = re.compile("|".join(SQLI_PATTERNS), re.IGNORECASE)

    print(f"########################{arg}#########################")

    if bool(SQLI_REGEX.search(arg)):
        raise ValueError(
            "Wrong argument."
        )
        # No more details, in order not to give more information
        # to a potentiel problematic user
