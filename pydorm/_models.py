from dataclasses import field, make_dataclass

from loguru import logger

from ._datasources import get_datasource


class Models:
    def __init__(self):
        self._model_dict: dict[str, callable] = {}
        self._table_structure_dict: dict[str, list[TableField]] = {}

    def get_structure(
        self, ds_id: str, database: str | None, table: str
    ) -> list["TableField"]:
        ds = get_datasource(ds_id)
        if database is None:
            database = ds.get_default_database()

        self.get(ds_id, database, table)

        key = f"{ds_id}.{database}.{table}"
        return self._table_structure_dict.get(key, None)

    def get(self, ds_id: str, database: str | None, table: str) -> callable:
        ds = get_datasource(ds_id)
        if database is None:
            database = ds.get_default_database()

        key = f"{ds_id}.{database}.{table}"
        model = self._model_dict.get(key, None)
        if model is None:
            return self._load_structure(ds_id, database, table)
        else:
            return model

    def remove(self, ds_id: str, database: str | None, table: str) -> None:
        ds = get_datasource(ds_id)
        if database is None:
            database = ds.get_default_database()

        key = f"{ds_id}.{database}.{table}"
        if key in self._model_dict:
            del self._model_dict[key]
        if key in self._table_structure_dict:
            del self._table_structure_dict[key]

    def _load_structure(self, ds_id: str, database: str | None, table: str):
        ds = get_datasource(ds_id)
        conn = ds.create_connection()
        c = conn.cursor()
        key = f"{ds_id}.{database}.{table}"
        try:
            sql = f"show full columns from {database}.{table}"
            logger.debug(f"[{id(conn)}] {sql}")

            c.execute(sql)
            rows = c.fetchall()
            table_fields = []
            for row in rows:
                table_field = TableField(
                    field_=row["Field"],
                    type_=row["Type"],
                    null_=row["Null"],
                    key_=row["Key"],
                    default_=row["Default"],
                    extra=row["Extra"],
                    comment=row.get("Comment", ""),
                )
                table_fields.append(table_field)
            self._table_structure_dict[key] = table_fields
            fields = [
                (table_field.field_, any, field(default=None))
                for table_field in table_fields
            ]
            self._model_dict[key] = make_dataclass(key, fields=fields)
            return self._model_dict[key]
        finally:
            c.close()
            conn.commit()


class TableField:

    def __init__(self, field_, type_, null_, key_, default_, extra, comment):
        self.field_ = field_
        self.type_ = type_
        self.null_ = null_
        self.key_ = key_
        self.default_ = default_
        self.extra = extra
        self.comment = comment


models = Models()
