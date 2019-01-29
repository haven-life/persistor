// third party modules
import * as _ from 'underscore';
import * as knexImport from 'knex';
// internal modules
import { UtilityFunctions } from './UtilityFunctions';
import { PersistObjectTemplate } from './PersistObjectTemplate';


export namespace Knex {
    export async function getPOJOsFromKnexQuery(persistor: typeof PersistObjectTemplate, template, joins, queryOrChains, options, map, customLogger, projection?) {

        const logger = customLogger || this.logger;
        const tableName = UtilityFunctions.dealias(template.__table__);
        const queryBuilder = (UtilityFunctions.getDB(persistor, UtilityFunctions.getDBAlias(template.__table__)).connection as knexImport)(tableName);

        const columnNames = getColumnNames(persistor, template, joins, projection);

        // tack on outer joins.  All our joins are outerjoins and to the right.  There could in theory be
        // foreign keys pointing to rows that no longer exists
        let select = queryBuilder.select(columnNames).from(tableName);

        joins.forEach((join) => {
            const joinTableName = UtilityFunctions.dealias(join.template.__table__);
            const tableName = UtilityFunctions.dealias(template.__table__);
            select = select.leftOuterJoin(`${joinTableName} as ${join.alias}`, `${join.alias}.${join.parentKey}`, `${tableName}.${join.childKey}`);
        });

        // execute callback to chain on filter functions or convert mongo style filters
        if (queryOrChains) {
            if (typeof(queryOrChains) == 'function') {
                queryOrChains(select);
            }
            else if (queryOrChains) {
                select = this.convertMongoQueryToChains(tableName, select, queryOrChains);
            }
        }

        if (options && options.sort) {
            const ascending: string[] = [];
            const descending = [];

            _.each(options.sort, (value, key) => {
                if (value > 0) {
                    ascending.push(tableName + '.' + key);
                }
                else {
                    descending.push(tableName + '.' + key);
                }
            });

            // @TODO: ask srksag about conflicting types
            if (ascending.length) {
                select = select.orderBy(ascending);
            }
            if (descending.length) {
                select = select.orderBy(descending, 'DESC');
            }
        }

        if (options && options.limit) {
            select = select.limit(options.limit);
            select = select.offset(0)
        }

        if (options && options.offset) {
            select = select.offset(options.offset);
        }

        logger.debug(
            {
                component: 'persistor',
                module: 'db.getPOJOsFromKnexQuery',
                activity: 'pre',
                data: {
                    template: template.__name__,
                    query: queryOrChains
                }
            });

        const selectString = select.toString();

        if (map && map[selectString]) {
            // @TODO: what's going on here
            return map[selectString].push(await Promise.resolve());
            // return new Promise(function (resolve) {
            //     map[selectString].push(resolve);
            // });
        }

        if (map) {
            map[selectString] = [];
        }

        return select
            .then(processResults.bind(this), processError.bind(this));


        function processResults(res) {
            logger.debug({component: 'persistor', module: 'db.getPOJOsFromKnexQuery', activity: 'post',
                data: {count: res.length, template: template.__name__, query: queryOrChains}});
            if (map && map[selectString]) {
                map[selectString].forEach(function(resolve) {
                    resolve(res)
                });
                delete map[selectString];
            }
            return res;
        }

        function processError(err) {
            logger.debug({component: 'persistor', module: 'db.getPOJOsFromKnexQuery', activity: 'select',
                error: JSON.stringify(err)});
            throw err;
        }
    }

    /**
     * Get the count of rows
     *
     * @param {object} template super type
     * @param {object/function} queryOrChains conditions to use, can even pass functions to add extra conditions
     * @param {object} _logger objecttemplate logger
     * @returns {*}
     */
    export async function countFromKnexQuery(persistor: typeof PersistObjectTemplate, template, queryOrChains, _logger) {
        const tableName = UtilityFunctions.dealias(template.__table__);
        const knex = UtilityFunctions.getDB(persistor, this.getDBAlias(template.__table__)).connection(tableName);
        // execute callback to chain on filter functions or convert mongo style filters
        if (typeof(queryOrChains) == 'function')
            queryOrChains(knex);
        else if (queryOrChains)
            (this.convertMongoQueryToChains)(tableName, knex, queryOrChains);

        const countResult = await knex.count('_id')
        return countResult[0].count * 1;
    }

    /**
     *Check for table existence
     *
     * @param {object} template super type
     * @param {string} tableName table to search on the database.
     * @returns {*}
     */
    export async function checkForKnexTable(persistor: typeof PersistObjectTemplate, template, tableName): Promise<boolean> {
        tableName = tableName ? tableName : this.dealias(template.__table__);
        const knexInstance = UtilityFunctions.getDB(persistor, UtilityFunctions.getDBAlias(template.__table__)).connection as knexImport;

        return await knexInstance.schema.hasTable(tableName);
    }

    /**
     * Check for column type in the database
     * @param {object} template super type
     * @param {string} column column to search.
     * @returns {*}
     */
    export function checkForKnexColumnType(persistor: typeof PersistObjectTemplate, template, column) {
        const tableName = UtilityFunctions.dealias(template.__table__);
        const knexInstance = UtilityFunctions.getDB(persistor, UtilityFunctions.getDBAlias(template.__table__)).connection as knexImport;

        const columnMetaData = await knexInstance(tableName).columnInfo(column);

        return columnMetaData.type;
    }

    /**
     * Drop the index if exists, tries to delete the constrain if the givne name is not an index.
     * @param {object} template supertype
     * @param {string} indexName index name to drop
     * @constructor
     */
    export async function dropIfKnexIndexExists(persistor: typeof PersistObjectTemplate, template, indexName) {
        const tableName = UtilityFunctions.dealias(template.__table__);
        const knex = UtilityFunctions.getDB(persistor, this.getDBAlias(template.__table__)).connection as knexImport;

        if (indexName.indexOf('idx_') === -1) {
            indexName = `idx_${tableName}_indexName`;
        }
        
        try {
            return await knex.schema.table(tableName, (table) => {
                table.dropIndex([], indexName);
            });
        }
        catch (err) {
            return await knex.schema.table(tableName, (table) => {
                table.dropUnique([], indexName);
            })
        }
    }

    /**
     * Delete Rows
     *
     * @param {object} template supertype
     * @param {object/function} queryOrChains conditions to use, can even pass functions to add extra conditions
     * @param {object} txn transaction object
     * @param {object} _logger objecttemplate logger
     * @returns {*}
     */
    export function deleteFromKnexQuery(persistor: typeof PersistObjectTemplate, template, queryOrChains, txn, _logger) {
        const tableName = UtilityFunctions.dealias(template.__table__);
        const knex = UtilityFunctions.getDB(persistor, this.getDBAlias(template.__table__)).connection as knexImport;

        if (txn && txn.knex) {
            // Compiler error @TODO:???
            knex.transacting(txn.knex);
        }

        // execute callback to chain on filter functions or convert mongo style filters
        if (typeof(queryOrChains) == 'function') {
            queryOrChains(knex);
        }
        else if (queryOrChains) {
            (this.convertMongoQueryToChains)(tableName, knex, queryOrChains);
        }

        return knex.delete();
    }

    export function deleteFromKnexByQuery(template, queryOrChains, txn, _logger) {
        const deleteQueries = txn ? txn.deleteQueries : this.deleteQueries;
        deleteQueries[template.__name__] = {name: template.__name__, template: template, queryOrChains: queryOrChains};
        txn.deleteQueries = deleteQueries;
    }

    export function knexPruneOrphans (persistor: typeof PersistObjectTemplate, obj, property, txn, filterKey, filterValue, logger?) {
        const template = obj.__template__;
        const defineProperty = template.getProperties()[property];

        const tableName = UtilityFunctions.dealias(template.__table__);
        let knex: knexImport.QueryBuilder = UtilityFunctions.getDB(persistor, this.getDBAlias(template.__table__)).connection as knexImport; // should be Knex instance

        if (txn && txn.knex) {
            knex.transacting(txn.knex);
        }
        const foreignKey = template.__schema__.children[property].id;
        var goodList = [];

        _.each(obj[property], function(o: any) {
            if (o._id)
                goodList.push(o._id);
        });

        knex = (goodList.length > 0 ? knex.whereNotIn('_id', goodList) : knex);
        knex = knex.andWhere(foreignKey, obj._id);
        knex = (filterKey ? knex.andWhere(filterKey, filterValue) : knex);
        knex = knex.delete().then(function (res) {
            if (res)
                (logger || this.logger).debug({component: 'persistor', module: 'db.knexPruneOrphans', activity: 'post',
                    data: {count: res, table: tableName, id: obj._id}});
        }.bind(this));

        return knex;
    };


        /**
     * Delete a Row
     *
     * @param {object} template supertype
     * @param {string} id primary key
     * @param {object} txn transaction object
     * @param {object} _logger objecttemplate logger
     * @returns {*}
     */
    export function deleteFromKnexId (template, id, txn, _logger) {

        var tableName = this.dealias(template.__table__);
        var knex = this.getDB(this.getDBAlias(template.__table__)).connection(tableName);
        if (txn && txn.knex) {
            knex.transacting(txn.knex);
        }
        return knex.where({_id: id}).delete();
    };

}









// -------------------------------------------------
// INTERNAL HELPER FUNCTIONS
// -------------------------------------------------

function getColumnNames(persistor: typeof PersistObjectTemplate, template, joins: any, projection) {
    const cols = [];

    while (template.__parent__)
        template = template.__parent__;

    asStandard(persistor, template, UtilityFunctions.dealias(template.__table__), projection, cols);
    _.each(getPropsRecursive(template), function (defineProperties, prop) {
        as(persistor, template, UtilityFunctions.dealias(template.__table__), prop, defineProperties, projection, cols)
    });

    _.each(joins, function (join: any) {
        asStandard(persistor, join.template, join.alias, projection, cols);
        _.each(getPropsRecursive(join.template), function (defineProperties, prop) {
            as(persistor, join.template, join.alias, prop, defineProperties, projection, cols)
        })
    });

    return cols;
}

function getPropsRecursive(template, map?) {
    map = map || {};
    _.map(template.getProperties(), function getPropsRecursiveMapper(val, prop) {
        map[prop] = val
    });
    template = template.__children__;
    template.forEach(function (template) {
        getPropsRecursive(template, map);
    });
    return map;
}

function asStandard(persistor, template, prefix, projection, cols) {
    as(persistor, template, prefix, '__version__', {type: {}, persist: true, enumerable: true}, projection, cols);
    as(persistor, template, prefix, '_template', {type: {}, persist: true, enumerable: true}, projection, cols);
    as(persistor, template, prefix, '_id', {type: {}, persist: true, enumerable: true}, projection, cols);
}

function as(persistor, template, prefix, prop, defineProperty, projection, cols) {
    const schema = template.__schema__;
    const type = defineProperty.type;
    const of = defineProperty.of;
    if (!UtilityFunctions._persistProperty(persistor, defineProperty) || !defineProperty.enumerable)
        return;
    if (type == Array && of.__table__) {
        return;
    }
    if (!prop.match(/^_./i) && !type.isObjectTemplate && !!projection && projection[template.__name__] instanceof Array && projection[template.__name__].indexOf(prop) === -1) {
        return;
    }
    else if (type.isObjectTemplate) {
        if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id)
            throw  new Error(type.__name__ + '.' + prop + ' is missing a parents schema entry');
        prop = schema.parents[prop].id;
    }
    cols.push(prefix + '.' + prop + ' as ' + (prefix ? prefix + '___' : '') + prop);
}