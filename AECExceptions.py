class LevelTooLowError(Exception):
    """
    This exception will be raised when the current level is below the minimum level.
    """
    pass

class LevelTooHighError(Exception):
    """
    This exception will be raised when the current level is above the maximum level.
    """
    pass

class MaxVolumeExceededError(Exception):
    """
    This exception will be raised when the target volume exceeds the maximum volume available for remainder of the day.
    """
    pass

class TargetNotSatisfiedError(Exception):
    """
    This exception will be raised when the pumped volume has not reached the target volume.
    """
    pass
