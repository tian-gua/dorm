from dataclasses import field, make_dataclass

from loguru import logger

from pydorm.protocols import IDataSource


class Models:
    def __init__(self):
        self._model_dict: dict[str, callable] = {}
        self._table_structure_dict: dict[str, list[TableField]] = {}

    def get(self, data_source: IDataSource, database: str, table: str) -> callable:
        data_source_id = data_source.get_id()
        key = f'{data_source_id}.{database}.{table}'
        model = self._model_dict.get(key, None)
        if model is None:
            conn = data_source.get_connection()
            c = conn.cursor()
            try:
                sql = f'show columns from {database}.{table}'
                logger.debug(f'[{id(conn)}] {sql}')

                c.execute(sql)
                rows = c.fetchall()
                table_fields = []
                for row in rows:
                    table_field = TableField(field_=row['Field'],
                                             type_=row['Type'],
                                             null_=row['Null'],
                                             key_=row['Key'],
                                             default_=row['Default'],
                                             extra=row['Extra'])
                    table_fields.append(table_field)
                self._table_structure_dict[key] = table_fields
                fields = [(table_field.field_, any, field(default=None)) for table_field in table_fields]
                self._model_dict[key] = make_dataclass(key, fields=fields)
                return self._model_dict[key]
            finally:
                c.close()
                conn.commit()
        else:
            return model


class TableField:

    def __init__(self, field_, type_, null_, key_, default_, extra):
        self.field_ = field_
        self.type_ = type_
        self.null_ = null_
        self.key_ = key_
        self.default_ = default_
        self.extra = extra


models = Models()
