from datetime import datetime, date

class TimeEval:
    """
    Class to evaluate the datetime of an event
    """
    def __init__(self, date_to_eval: datetime) -> None:
        """
        Constructor of the TimeEval class
        :param date_to_eval: datetime -> date time to evaluate
        """
        self.date_to_eval = date_to_eval

    def is_weekend(self) -> bool:
        """
        :return: True if  the datetime is a weekend day, False if it's not
        """
        return self.date_to_eval.weekday() > 4

    def is_business(self) -> bool:
        """
        :return: True if a datetime is morning, false if it's not
        """
        return  self.date_to_eval.hour in range(7,20)

