# create entity_linking.db
# 2023/6/1, Tadashi masuda

from src.PathClass import PathClass
from src.UriClass import Uri


if __name__ == '__main__':
    path = PathClass('')
    uri = Uri(path, 'data_set2', 'URI')
    uri.create_entity_linking_db()

    uri.build_entity_linking()
    uri.test_entity_linking()
