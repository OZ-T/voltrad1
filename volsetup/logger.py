import logging

class logger(object):
    def __init__(self,name1):
        # create logger
        self.logger = logging.getLogger(name1)
        self.logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.DEBUG)

        # create formatter
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # add formatter to ch
        self.ch.setFormatter(self.formatter)

        # add ch to logger
        self.logger.addHandler(self.ch)

    def info(self,str):
        self.logger.info(str)


    def debug(self, str):
        self.logger.debug(str)

    def warn(self, str):
        self.logger.warn(str)


    def error(self, str):
        self.logger.error(str)


    def critical(self, str):
        self.logger.critical(str)


if __name__ == "__main__":
    log=logger("teto1")
    log.info("KKKKKK")
    log.critical("KKKKKK")