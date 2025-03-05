def build_select(query_wrapper) -> tuple[str, tuple[any, ...]]:
    sql = f'SELECT {",".join(query_wrapper.field_list)} FROM {query_wrapper.table}'
    args = ()
    if len(query_wrapper.condition_tree.conditions) > 0:
        exp, args = query_wrapper.condition_tree.parse()
        sql += ' WHERE ' + exp
    if query_wrapper.order_by is not None:
        sql += f' ORDER BY {",".join(query_wrapper.order_by)}'
    if query_wrapper.limit_ is not None:
        sql += f' LIMIT {query_wrapper.limit_}'
    if query_wrapper.offset is not None:
        sql += f' OFFSET {query_wrapper.offset}'

    return sql, args
