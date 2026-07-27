# Example of multi-file Python game logic via a normal local import.
#
# examples/python-game is added to sys.path when main.py is loaded, so
# `from systems import roster` resolves to this module.

_players = set()


def add(participant_id):
    """Record a participant as present. Returns the new online count."""
    _players.add(participant_id)
    return len(_players)


def remove(participant_id):
    """Remove a participant. Returns the new online count."""
    _players.discard(participant_id)
    return len(_players)


def count():
    """How many participants are currently online."""
    return len(_players)
