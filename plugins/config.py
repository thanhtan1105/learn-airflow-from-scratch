import yaml
import os


class Config:
    def __init__(self):
        env = os.getenv('ENV', 'dev')
        with open(os.path.join(os.path.dirname(__file__), f'config/config.{env}.yml')) as f:
            self.config = yaml.safe_load(f)
    def get_config(self):
        return self.config


