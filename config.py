import yaml
class configReader:
    def __init__(self, config_path:str = './config/config.yaml') -> None:
        """
        Constructor of the config reader class that generates a config object containing all the
        parameters that are needed and defined in a yaml config file that should be stored under
        ./config/config.yaml
        :param config_path: str -> path to config file
        """
        self.config_path = config_path
        self.configObject = None

    def generateConfigObject(self) -> None:
        """
        Method that populates the config object containing all needed parameters
        """
        with open (self.config_path, 'r') as f:
            self.configObject = yaml.safe_load(f)
