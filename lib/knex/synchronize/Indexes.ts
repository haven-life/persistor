import * as Knex from 'knex';

export namespace Indexes {
    export function synchronizeIndexes (persistor, template) {
        var aliasedTableName = template.__table__;
        tableName = this.dealias(aliasedTableName);

        while (template.__parent__) {
            template =  template.__parent__;
        }

        if (!template.__table__) {
            throw new Error(`${template.__name__} is missing a schema entry`);
        }
        var knex = persistor.getDB(persistor.getDBAlias(template.__table__)).connection;
        var schema = this._schema;

        var _dbschema;
        var _changes =  {};
        var schemaTable = 'index_schema_history';
        var schemaField = 'schema';

        return Promise.resolve()
            .then(loadSchema.bind(this, tableName))
            .spread(loadTableDef)
            .spread(diffTable)
            .then(generateChanges.bind(this, template))
            .then(mergeChanges)
            .then(applyTableChanges)
            .then(makeSchemaUpdates)
            .catch(function(e) {
                throw e;
            })
    }

    async function loadSchema (tableName: string, knex: Knex, _dbSchema) {
        if (!!_dbSchema) {
            return (_dbSchema, tableName); // @TODO: What does this return;
        }

        const exists = await knex.schema.hasTable(schemaTable);

        // create
        if (!exists) {
            knex.schema.createTable(schemaTable, (table) => { // Do we do anything with this?
                table.increments('sequence_id').primary();
                table.text(schemaField);
                table.timestamps();
            })
        }

    }
}