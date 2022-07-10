import logging as lg


class Log:
    def __init__(self):
        try:
            self.logFile = "CRUD_thru_APIs.log"

            # removing the log file if already exists so as not to congest it.
            if os.path.exists(self.logFile):
                os.remove(self.logFile)
            lg.basicConfig(filename=self.logFile, level=lg.INFO, format="%(asctime)s %(levelname)s %(message)s")

            # Adding the StreamHandler to record logs in the console.
            self.console_log = lg.StreamHandler()
            self.console_log.setLevel(lg.INFO)  # setting level to the console log.
            self.format = lg.Formatter("%(asctime)s %(message)s")
            self.console_log.setFormatter(self.format)  # defining format for the console log.
            lg.getLogger('').addHandler(self.console_log)  # adding handler to the console log.

        except Exception as e:
            lg.info(e)

        else:
            lg.info("Log Class successfully executed!")
