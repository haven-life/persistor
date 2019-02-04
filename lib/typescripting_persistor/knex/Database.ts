import * as _ from 'underscore';
import { PersistObjectTemplate } from '../PersistObjectTemplate';
import { UtilityFunctions } from '../UtilityFunctions';
import { Persistent, PersistentConstructor } from '../Persistent';
import * as knexImport from 'knex';
import { SupertypeConstructor } from 'supertype';

export namespace Database {

    // TODO NICK why do we need this to be here?
    const processedList = [];
    export async function getPOJOsFromKnexQuery(persistor: typeof PersistObjectTemplate, template, joins, queryOrChains, options, map, customLogger, projection?) {

        const logger = customLogger || persistor.logger;
        const tableName: string = UtilityFunctions.dealias(template.__table__);
        const knexRef: knexImport = UtilityFunctions.getKnexConnection(persistor, template);
        const queryBuilder = knexRef(tableName);

        const columnNames = getColumnNames(persistor, template, joins, projection);

        // tack on outer joins.  All our joins are outerjoins and to the right.  There could in theory be
        // foreign keys pointing to rows that no longer exists
        let select = queryBuilder.select(columnNames).from(tableName);

        joins.forEach((join) => {
            const joinTableName = UtilityFunctions.dealias(join.template.__table__);
            const tableName = UtilityFunctions.dealias(template.__table__);
            select = select.leftOuterJoin(`${joinTableName} as ${join.alias}`, `${join.alias}.${join.parentKey}`, `${tableName}.${join.childKey}`);
        });

        select = executeQueryOrChains(queryOrChains, select, tableName);

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
                for (const asc of ascending) {
                    select = select.orderBy(asc);
                }
            }
            if (descending.length) {
                for (const desc of ascending) {
                    select = select.orderBy(desc);
                }
                select = select.orderBy('DESC');
            }
        }

        if (options && options.limit) {
            select = select.limit(options.limit);
            select = select.offset(0)
        }

        if (options && options.offset) {
            select = select.offset(options.offset);
        }

        logger.debug({
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

        return await select
            .then(processResults)
            .then(processError);


        function processResults(res) {
            logger.debug({
                component: 'persistor', module: 'db.getPOJOsFromKnexQuery', activity: 'post',
                data: { count: res.length, template: template.__name__, query: queryOrChains }
            });
            if (map && map[selectString]) {
                map[selectString].forEach(function (resolve) {
                    resolve(res)
                });
                delete map[selectString];
            }
            return res;
        }

        function processError(err) {
            logger.debug({
                component: 'persistor', module: 'db.getPOJOsFromKnexQuery', activity: 'select',
                error: JSON.stringify(err)
            });
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
        let knex = UtilityFunctions.getKnexConnection(persistor, template)(tableName);

        knex = executeQueryOrChains(queryOrChains, knex, tableName);

        const countResult = await knex.count('_id');
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
        tableName = tableName ? tableName : UtilityFunctions.dealias(template.__table__);
        const knex = UtilityFunctions.getKnexConnection(persistor, template);
        return knex.schema.hasTable(tableName);
    }

    /**
     * Check for column type in the database
     * @param {object} template super type
     * @param {string} column column to search.
     * @returns {*}
     */
    export async function checkForKnexColumnType(persistor: typeof PersistObjectTemplate, template, column) {
        const tableName = UtilityFunctions.dealias(template.__table__);
        const knex = UtilityFunctions.getKnexConnection(persistor, template);

        const columnMetaData = await knex(tableName).columnInfo(column);

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
        const knex = UtilityFunctions.getKnexConnection(persistor, template)

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
    export async function deleteFromKnexQuery(persistor: typeof PersistObjectTemplate, template, queryOrChains, txn, _logger) {
        const tableName = UtilityFunctions.dealias(template.__table__);
        let knex = UtilityFunctions.getKnexConnection(persistor, template)(tableName);

        if (txn && txn.knex) {
            // Compiler error @TODO:???
            knex.transacting(txn.knex);
        }

        knex = executeQueryOrChains(queryOrChains, knex, tableName);

        return knex.delete();
    }

    export async function deleteFromKnexByQuery(persistor: typeof PersistObjectTemplate, template, queryOrChains, txn) {
        const deleteQueries = txn ? txn.deleteQueries : persistor.deleteQueries;
        deleteQueries[template.__name__] = { name: template.__name__, template: template, queryOrChains: queryOrChains };
        txn.deleteQueries = deleteQueries;
    }

    export async function knexPruneOrphans(persistor: typeof PersistObjectTemplate, obj, property, txn, filterKey, filterValue, logger?) {
        const template = obj.__template__;

        const tableName = UtilityFunctions.dealias(template.__table__);
        let knex = UtilityFunctions.getKnexConnection(persistor, template)(tableName);

        if (txn && txn.knex) {
            knex.transacting(txn.knex);
        }
        const foreignKey = template.__schema__.children[property].id;
        const goodList = [];

        _.each(obj[property], function (o: any) {
            if (o._id)
                goodList.push(o._id);
        });

        knex = (goodList.length > 0 ? knex.whereNotIn('_id', goodList) : knex);
        knex = knex.andWhere(foreignKey, obj._id);
        knex = (filterKey ? knex.andWhere(filterKey, filterValue) : knex);
        const result = await knex.delete();

        if (result) {
            logger.debug({
                component: 'persistor', module: 'db.knexPruneOrphans', activity: 'post',
                data: { count: result, table: tableName, id: obj._id }
            });
        }

        return;
    }

    /**
     * Delete a Row
     *
     * @param {object} template supertype
     * @param {string} id primary key
     * @param {object} txn transaction object
     * @param {object} _logger objecttemplate logger
     * @returns {*}
     */
    export async function deleteFromKnexId(persistor: typeof PersistObjectTemplate, template, id, txn, _logger) {
        const tableName = UtilityFunctions.dealias(template.__table__);
        let knex = UtilityFunctions.getKnexConnection(persistor, template)(tableName);

        if (txn && txn.knex) {
            knex.transacting(txn.knex);
        }

        return knex.where({ _id: id }).delete();
    }

    /**
     * Save a Plain Old Javascript object given an Id.
     * @param {object} obj supertype
     * @param {string} pojo primary key
     * @param {string} updateID primary key if updated..
     * @param {object} txn transaction object
     * @param {object} logger objecttemplate logger
     * @returns {*}
     */
    export async function saveKnexPojo(persistor: typeof PersistObjectTemplate, obj, pojo, updateID, txn, logger) {
        const origVer = obj.__version__;

        const tableName = UtilityFunctions.dealias(obj.__template__.__table__);
        const knex = UtilityFunctions.getKnexConnection(persistor, obj.__template__)(tableName);

        obj.__version__ = obj.__version__ ? obj.__version__ * 1 + 1 : 1;
        pojo.__version__ = obj.__version__;
        logger.debug({
            component: 'persistor', module: 'db.saveKnexPojo', activity: 'pre',
            data: {
                txn: (txn ? txn.id + ' ' : '-#- '), type: (updateID ? 'updating ' : 'insert '),
                template: obj.__template__.__name__, id: obj.__id__, _id: obj._id, __version__: pojo.__version__
            }
        });
        if (txn && txn.knex) {
            knex.transacting(txn.knex)
        }
        if (updateID) {
            const countUpdated = await knex.where('__version__', '=', origVer).andWhere('_id', '=', updateID).update(pojo);
            checkUpdateResults(countUpdated)
            logSuccess()
        } else {
            await knex.insert(pojo);
            logSuccess();
        }


        function checkUpdateResults(countUpdated) {
            if (countUpdated < 1) {
                logger.debug({
                    component: 'persistor', module: 'db.saveKnexPojo', activity: 'updateConflict',
                    data: { txn: (txn ? txn.id : '-#-'), id: obj.__id__, __version__: origVer }
                });
                obj.__version__ = origVer;
                if (txn && txn.onUpdateConflict) {
                    txn.onUpdateConflict(obj);
                    txn.updateConflict = true;
                } else {
                    throw new Error('Update Conflict');
                }
            }
        }

        function logSuccess() {
            logger.debug({
                component: 'persistor', module: 'db.saveKnexPojo', activity: 'post',
                data: { template: obj.__template__.__name__, table: obj.__template__.__table__, __version__: obj.__version__ }
            });
        }
    }

    /**
     * tries to synchronize the POJO model updates to the table definition.
     * e.g. adding a new field will add a field to the table.
     * @param {object} template supertype
     * @param {function} changeNotificationCallback callback to get the information on table or fields changes.
     * @param {bool} forceSync forces the function to proceed with sync table step, useful for unit tests.
     * @returns {*}
     */
    export async function synchronizeKnexTableFromTemplate(persistor: typeof PersistObjectTemplate, template: typeof Persistent, changeNotificationCallback?, forceSync?: boolean) {
        const aliasedTableName = template.__table__;
        const tableName = UtilityFunctions.dealias(aliasedTableName);

        /*
            TODO NICK refactor this to have some better way of figuring this information out
            e.g. query classes extend from some "query" class that has a query prop that says "I'm
            not a class that require synchronization"
         */
        //no need to synchronize Query objects if there is an entry for the corresponding main object in schema.json
        if (template.name.match(/Query$/) && isTableCorrespondsToOtherSchemaEntry(persistor, template.name, tableName)) {
            return;
        }

        while (template.__parent__) {
            template = template.__parent__;
        }

        // can skip the templates that were already processed.
        if (processedList.includes(template.__name__) && !forceSync) {
            return;
        }

        processedList.push(template.__name__);

        if (!template.__table__) {
            throw new Error(template.__name__ + ' is missing a schema entry');
        }

        const props = getPropsRecursive(template);
        const knex = UtilityFunctions.getKnexConnection(persistor, template);

        const schema = template.__schema__;
        const _newFields = {};

        await buildTable();
        await addComments(tableName);
        return await synchronizeIndexes(persistor, tableName, template);

        async function buildTable() {
            const exists = await knex.schema.hasTable(tableName);
            if (!exists) {
                if (!!changeNotificationCallback) {
                    if (typeof changeNotificationCallback !== 'function') {
                        throw new Error('Persistor can only notify the table changes through a callback');
                    }
                    else {
                        await changeNotificationCallback(`A new table, ${tableName}, has been added\n`);
                    }
                }

                return await createKnexTable(persistor, template, aliasedTableName);
            }

            await discoverColumns(tableName);
            fieldChangeNotify(changeNotificationCallback, tableName);
            return await knex.schema.table(tableName, columnMapper);
        }

        function fieldChangeNotify(callBack, table) {
            if (!callBack) return;
            if (typeof callBack !== 'function')
                throw new Error('persistor can only notify the field changes through a callback');
            const fieldsChanged = _.reduce(_newFields, function (current, field: any, key) {
                // TODO NICK how do we know what this is?
                return field.type !== Array ? current + ',' + key : current;
            }, '');

            if (fieldsChanged.length > 0) {
                callBack('Following fields are being added to ' + table + ' table: \n ' + fieldsChanged.slice(1, fieldsChanged.length));
            }
        }

        function columnMapper(table): void {

            for (const prop in _newFields) {
                const defineProperty = props[prop];

                if (!UtilityFunctions._persistProperty(defineProperty) || !defineProperty.enumerable)
                    continue;

                if (defineProperty.type === Array) {
                    if (!defineProperty.of.__objectTemplate__)
                        table.text(prop);
                } else if (defineProperty.type.__objectTemplate__) {

                    if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id) {
                        throw new Error(defineProperty.type.__name__ + '.' + prop + ' is missing a parents schema entry');
                    }
                    const foreignKey = (schema.parents && schema.parents[prop]) ? schema.parents[prop].id : prop;

                    table.text(foreignKey);
                } else if (defineProperty.type === Number) {
                    table.double(prop);
                } else if (defineProperty.type === Date) {
                    table.timestamp(prop);
                } else if (defineProperty.type === Boolean) {
                    table.boolean(prop);
                } else {
                    table.text(prop);
                }
            }
        }

        async function addComments(table: string) {
            const info = await knex(table).columnInfo();

            const promises = [];
            for (const columnName in info) {

                // No awaiting here apparently, need to doublecheck
                const knexCommentPromise = processComment(columnName);
                if (!!knexCommentPromise) {
                    promises.push(knexCommentPromise);
                }
            }

            return await Promise.all(promises);

            async function processComment(columnName): Promise<void> {
                let prop = columnNameToProp(columnName);

                if (!prop) {
                    PersistObjectTemplate.logger.info({ component: 'persistor', module: 'db.synchronizeKnexTableFromTemplate', activity: 'discoverColumns' }, 'Extra column ' + columnName + ' on ' + table);
                    return await commentOn(table, columnName, 'now obsolete');
                }

                if (prop === '_id') {
                    return await commentOn(table, columnName, 'primary key');
                } else if (prop.match(/:/)) {
                    prop = prop.substr(1);
                    const fkComment = getForeignKeyDescription(props[prop]);
                    const commentField = getDescription(prop, props[prop]);
                    const comment = (commentField === '') ? fkComment : fkComment + ', ' + commentField;
                    return await commentOn(table, columnName, comment);
                } else if (prop === '_template') {
                    return await commentOn(table, columnName, getClassNames());
                } else if (prop !== '__version__') {
                    return await commentOn(table, columnName, getDescription(prop, props[prop]));
                }
            }

            function columnNameToProp(columnName): string {
                if (columnName === '_id' || columnName === '__version__' || columnName === '_template') {
                    return columnName;
                }
                if (props[columnName]) {
                    return columnName;
                }

                if (!schema || !schema.parents) {
                    return;
                }

                for (const parent in schema.parents) {
                    if (columnName == schema.parents[parent].id && !props[parent]) {
                        PersistObjectTemplate.logger.info({ component: 'persistor', module: 'db.synchronizeKnexTableFromTemplate', activity: 'discoverColumns' },
                            'schema out-of-sync: schema contains ' + columnName + ' on ' + table + ', which is not defined in the template');
                        return '';
                    }
                    else if (columnName == schema.parents[parent].id) {
                        return ':' + parent;
                    }
                }

                return;
            }

            function getForeignKeyDescription(defineProperty): string {
                if (!defineProperty.type) {
                    return '';
                }

                const prop = defineProperty.type.__name__;
                const template = PersistObjectTemplate.__dictionary__[prop];

                if (!template) {
                    return '';
                }

                return 'foreign key for ' + template.__table__;
            }

            function getClassNames(): string {
                let className = '';

                // recursively go through and grab
                getClassName(template);
                return className;

                function getClassName(template) {
                    className += (className.length > 0 ? ', ' + template.__name__ : 'values: ' + template.__name__);
                    if (template.__children__) {
                        _.each(template.__children__, getClassName);
                    }
                }
            }

            function getDescription(prop, defineProperty): string {
                if (!defineProperty) {
                    return '';
                }

                const values = {};
                let valStr = '';
                processValues(template);

                let comment: string = defineProperty.comment ? defineProperty.comment + '; ' : '';

                // if the property has been marked as sensitive data, replace.
                comment = defineProperty.sensitiveData ? comment + ';;sensitiveData;;' : comment;

                _.each(values, function (_val, key) { valStr += (valStr.length == 0 ? '' : ', ') + key });
                comment = valStr.length > 0 ? comment + 'values: ' + valStr : comment;

                return comment;

                function processValues(template: SupertypeConstructor) {
                    // what is this doing? is this the native js defineProperties? or are we overriding it for ourselves?
                    const defineProperty = template.defineProperties ? template.defineProperties[prop] : null;
                    if (defineProperty && defineProperty.values)
                        _.each(defineProperty.values, function (val, key) {
                            // TODO NICK figure out what this is doing and type appropriately.
                            let index: any = defineProperty.values instanceof Array ? val : key;
                            values[index] = true;
                        });
                    if (template.__children__) {
                        _.each(template.__children__, processValues);
                    }
                }
            }

            async function commentOn(table, column, comment): Promise<void> {
                if (knex.client.config.client === 'pg' && comment !== '') {
                    const rawSql = `COMMENT ON COLUMN "${table}"."${column}" IS '${comment.replace(/'/g, '\'\'')}';`;

                    const retVal = await knex.raw(rawSql);
                    console.log(retVal);
                }
            }
        }

        async function discoverColumns(table) {
            const info = await knex(table).columnInfo();

            for (const prop in props) {
                const defineProperty = props[prop];
                if (UtilityFunctions._persistProperty(defineProperty)) {
                    if (!info[propToColumnName(prop)]) {
                        _newFields[prop] = props[prop];
                    }
                    else {
                        if (!iscompatible(props[prop].type.name, info[propToColumnName(prop)].type)) {
                            throw new Error('Changing the type of ' + prop + ' on ' + table
                                + ', changing types for the fields is not allowed, please use scripts to make these changes');
                        }
                    }
                }
            }

            function propToColumnName(prop: string): string {
                const defineProperty = props[prop];
                if (defineProperty.type.__objectTemplate__)
                    if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id)
                        throw new Error(template.__name__ + '.' + prop + ' is missing a parents schema entry');
                    else
                        prop = (schema.parents && schema.parents[prop]) ? schema.parents[prop].id : prop;
                return prop;
            }
        }

        function isTableCorrespondsToOtherSchemaEntry(persistor: typeof PersistObjectTemplate, name, table) {
            if (persistor._schema[name.replace('Query', '')]) {
                return true;
            }

            const schemaEntry = Object.keys(persistor._schema).find(function (k) {
                return persistor._schema[k].table === table && k != name;
            });

            return Boolean(schemaEntry);
        }
    }

    async function synchronizeIndexes(persistor: typeof PersistObjectTemplate, tableName, template) {

        const aliasedTableName = template.__table__;
        tableName = UtilityFunctions.dealias(aliasedTableName);

        while (template.__parent__) {
            template = template.__parent__;
        }

        if (!template.__table__) {
            throw new Error(template.__name__ + ' is missing a schema entry');
        }

        const knex = UtilityFunctions.getKnexConnection(persistor, template);
        const schema = persistor._schema;

        let _dbschema;
        const _changes = {};
        const schemaTable = 'index_schema_history';
        const schemaField = 'schema';

        const loadSchema = async function (tableName): Promise<{ schema: any, tableName: any }> {
            let retVal: { schema: any, tableName: any };
            if (!!_dbschema) {
                //@ts-ignore
                return { retVal: _dbschema, tableName: tableName };
            }

            const exists = await knex.schema.hasTable(schemaTable);
            if (!exists) {
                await knex.schema.createTable(schemaTable, (table) => {
                    table.increments('sequence_id').primary();
                    table.text(schemaField);
                    table.timestamps();
                });
            }
            const record = await knex(schemaTable).orderBy('sequence_id', 'desc').limit(1);
            let response;
            if (!record[0]) {
                response = {};
            }
            else {
                response = JSON.parse(record[0][schemaField]);
            }
            _dbschema = response;
            return { schema: response, tableName: template.__name__ };
        };

        const loadTableDef = function (dbschema, tableName) {
            if (!dbschema[tableName]) {
                dbschema[tableName] = {};
            }
            return { dbschema, schema, tableName };
        };

        const diffTable = function (dbschema, schema, tableName) {
            const dbTableDef = dbschema[tableName];
            const memTableDef = schema[tableName];
            const track = { add: [], change: [], delete: [] };

            let addPredicate = function (_dbIdx, memIdx) {
                return !memIdx;
            };

            _diff(dbTableDef, memTableDef, 'delete', false, addPredicate, _diff(memTableDef, dbTableDef, 'change', false, (memIdx, dbIdx) => {
                return memIdx && dbIdx && !_.isEqual(memIdx, dbIdx);
            }, _diff(memTableDef, dbTableDef, 'add', true, (_memIdx, dbIdx) => {
                return !dbIdx;
            }, track)));

            _changes[tableName] = _changes[tableName] || {};

            _.map(_.keys(track), (key) => {
                _changes[tableName][key] = _changes[tableName][key] || [];
                _changes[tableName][key].push.apply(_changes[tableName][key], track[key]);
            });

            function _diff(masterTblSchema, shadowTblSchema, opr, addMissingTable, addPredicate, diffs) {

                if (!!masterTblSchema && !!masterTblSchema.indexes && masterTblSchema.indexes instanceof Array && !!shadowTblSchema) {
                    (masterTblSchema.indexes || []).forEach(function (mstIdx) {
                        const shdIdx = _.findWhere(shadowTblSchema.indexes, { name: mstIdx.name });

                        if (addPredicate(mstIdx, shdIdx)) {
                            diffs[opr] = diffs[opr] || [];
                            diffs[opr].push(mstIdx);
                        }
                    });
                } else if (addMissingTable && !!masterTblSchema && !!masterTblSchema.indexes) {
                    diffs[opr] = diffs[opr] || [];
                    diffs[opr].push.apply(diffs[opr], masterTblSchema.indexes);
                }
                return diffs;
            }
        };

        const generateChanges = (localTemplate) => {
            return _.reduce(localTemplate.__children__, (_curr: SupertypeConstructor, o: SupertypeConstructor) => {
                let tableDefinition = loadTableDef(_dbschema, o.__name__);
                let diff = diffTable(tableDefinition.dbschema, tableDefinition.schema, tableDefinition.tableName);
                return generateChanges(diff);
            }, {});
        };

        const getFilteredTarget = (src, target) => {
            // TODO NICK type this 'o' param
            return _.filter(target, function getFilteredTargetFilter(o: any) {
                const currName = _.reduce(o.def.columns, (name, col) => {
                    return `${name}_${col}`;
                }, 'idx_' + tableName);

                return !_.find(src, function getFilteredTargetFind(cached: any) {
                    const cachedName = _.reduce(cached.def.columns, (name, col) => {
                        return `${name}_${col}`;
                    }, 'idx_' + tableName);
                    return (cachedName.toLowerCase() === currName.toLowerCase());
                })
            });
        };

        const mergeChanges = function () {
            const dbChanges = { add: [], change: [], delete: [] };
            _.map(dbChanges, function (_object, key) {
                _.each(_changes, function (change) {
                    const uniqChanges = _.uniq(change[key], function (o: any) {
                        return o.name;
                    });
                    const filtered = getFilteredTarget(dbChanges[key], uniqChanges);
                    dbChanges[key].push.apply(dbChanges[key], filtered);
                })
            });

            return dbChanges;
        };

        const applyTableChanges = async function (dbChanges) {
            function syncIndexesForHierarchy(operation, diffs, table) {
                _.map(diffs[operation], (function applyTableChangesMap(object: any, _key) {
                    let type = object.def.type;
                    const columns = object.def.columns;
                    if (type !== 'unique' && type !== 'index')
                        throw new Error('index type can be only "unique" or "index"');

                    let name = _.reduce(object.def.columns, function (name, col) {
                        return name + '_' + col;
                    }, 'idx_' + tableName);

                    name = name.toLowerCase();
                    if (operation === 'add') {
                        table[type](columns, name);
                    }
                    else if (operation === 'delete') {
                        type = type.replace(/index/, 'Index');
                        type = type.replace(/unique/, 'Unique');
                        table['drop' + type]([], name);
                    }
                    else
                        table[type](columns, name);

                }));
            }

            return await knex.transaction(async (trx) => {
                return await trx.schema.table(tableName, (table) => {
                    _.map(Object.getOwnPropertyNames(dbChanges), (key) => {
                        return syncIndexesForHierarchy(key, dbChanges, table);
                    });
                });
            });
        };

        const isSchemaChanged = function (object) {
            return (object.add.length || object.change.length || object.delete.length)
        };

        const makeSchemaUpdates = async function () {
            const chgFound = _.reduce(_changes, function (curr, change) {
                return curr || !!isSchemaChanged(change);
            }, false);

            if (!chgFound) {
                return;
            }

            const record = await knex(schemaTable).orderBy('sequence_id', 'desc').limit(1);
            let response = {};
            let sequence_id;
            if (!record[0]) {
                sequence_id = 1;
            }
            else {
                response = JSON.parse(record[0][schemaField]);
                sequence_id = ++record[0].sequence_id;
            }
            _.each(_changes, function (_o, chgKey) {
                response[chgKey] = schema[chgKey];
            });

            return await knex(schemaTable).insert({
                sequence_id: sequence_id,
                schema: JSON.stringify(response)
            });
        }

        try {
            const loadedSchema = await loadSchema(tableName);
            const loadedTableDef = loadTableDef(loadedSchema.schema, loadedSchema.tableName);
            diffTable(loadedTableDef.dbschema, loadedTableDef.schema, loadedTableDef.tableName);
            generateChanges(template);
            const dbChanges = mergeChanges();
            await applyTableChanges(dbChanges);
            return await makeSchemaUpdates();
        }
        catch (e) {
            throw e;
        }
    }

    function getPropsRecursive(template, map?) {
        map = map || {};
        _.map(template.getProperties(), function getPropsRecursiveMapper(val, prop) {
            map[prop] = val
        });
        template = template.__children__;
        template.forEach((template) => {
            getPropsRecursive(template, map);
        });
        return map;
    }

    export async function persistTouchKnex(persistor: typeof PersistObjectTemplate, obj: Persistent, txn, logger) {
        logger.debug({
            component: 'persistor', module: 'db.persistTouchKnex', activity: 'pre',
            data: { template: obj.__template__.__name__, table: obj.__template__.__table__ }
        });
        const tableName = UtilityFunctions.dealias(obj.__template__.__table__);
        const knex = UtilityFunctions.getKnexConnection(persistor, obj.__template__)(tableName);

        obj.__version__++;
        if (txn && txn.knex) {
            knex.transacting(txn.knex)
        }

        await knex.where('_id', '=', obj._id).increment('__version__', 1);
        logger.debug({
            component: 'persistor', module: 'db.persistTouchKnex', activity: 'post',
            data: { template: obj.__template__.__name__, table: obj.__template__.__table__ }
        });

    }

    export async function createKnexTable(persistor: typeof PersistObjectTemplate, template, collection) {
        collection = collection || template.__table__;
        const tableName = UtilityFunctions.dealias(collection);
        await _createKnexTable(persistor, template, collection);
        return await synchronizeIndexes(persistor, tableName, template);
    }

    /**
     * Drop table if exists, just a wrapper method on Knex library.
     * @param {object} template super type
     * @param {string} tableName table to drop
     * @returns {*}
     */
    export function dropKnexTable(persistor: typeof PersistObjectTemplate, template: PersistentConstructor, tableName?) {
        const knex = UtilityFunctions.getKnexConnection(persistor, template);
        tableName = tableName ? tableName : UtilityFunctions.dealias(template.__table__);

        return knex.schema.dropTableIfExists(tableName);
    }


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

    function asStandard(persistor, template, prefix, projection, cols) {
        as(persistor, template, prefix, '__version__', { type: {}, persist: true, enumerable: true }, projection, cols);
        as(persistor, template, prefix, '_template', { type: {}, persist: true, enumerable: true }, projection, cols);
        as(persistor, template, prefix, '_id', { type: {}, persist: true, enumerable: true }, projection, cols);
    }

    function as(persistor, template, prefix, prop, defineProperty, projection, cols) {
        const schema = template.__schema__;
        const type = defineProperty.type;
        const of = defineProperty.of;

        if (!UtilityFunctions._persistProperty(defineProperty) || !defineProperty.enumerable) {
            return;
        }

        if (type == Array && of.__table__) {
            return;
        }

        if (!prop.match(/^_./i) && !type.isObjectTemplate && !!projection && projection[template.__name__] instanceof Array && projection[template.__name__].indexOf(prop) === -1) {
            return;
        }

        else if (type.isObjectTemplate) {
            if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id) {
                throw new Error(type.__name__ + '.' + prop + ' is missing a parents schema entry');
            }

            prop = schema.parents[prop].id;
        }

        cols.push(prefix + '.' + prop + ' as ' + (prefix ? prefix + '___' : '') + prop);
    }

    function iscompatible(persistortype, pgtype) {
        switch (persistortype) {
            case 'String':
            case 'Object':
            case 'Array':
                return pgtype.indexOf('text') > -1;
            case 'Number':
                return pgtype.indexOf('double precision') > -1;
            case 'Boolean':
                return pgtype.indexOf('bool') > -1;
            case 'Date':
                return pgtype.indexOf('timestamp') > -1;
            default:
                return pgtype.indexOf('text') > -1; // Typed objects have no name
        }
    }

    export async function _commitKnex(persistor: typeof PersistObjectTemplate, persistorTransaction, logger, notifyChanges) {
        logger.debug({ component: 'persistor', module: 'api', activity: 'commit' }, 'end of transaction ');
        const knex = (_.findWhere(persistor._db as any, { type: PersistObjectTemplate.DB_Knex }) as any).connection;
        const dirtyObjects = persistorTransaction.dirtyObjects;
        const touchObjects = persistorTransaction.touchObjects;
        const savedObjects = persistorTransaction.savedObjects;
        const deletedObjects = persistorTransaction.deletedObjects;
        const deleteQueries = persistorTransaction.deleteQueries;
        let innerError;
        let changeTracking;

        // Start the knext transaction
        try {
            await knex.transaction(async function (knexTransaction) {
                persistorTransaction.knex = knexTransaction;
                try {
                    await processPreSave();
                    await processSaves();
                    await processDeletes();
                    await processDeleteQueries(persistor);
                    await processTouches();
                    await processPostSave();
                    await processCommit();
                }
                catch (err) {
                    await rollback(err);
                }

                async function processPreSave() {
                    return persistorTransaction.preSave
                        ? await persistorTransaction.preSave.call(persistorTransaction, persistorTransaction, logger)
                        : true
                }

                // Walk through the dirty objects
                async function processSaves() {
                    const dirtyArray = _.toArray(dirtyObjects);
                    return await process(dirtyArray, callSave, generateChangesAction)

                    function generateChangesAction(obj) {
                        if (obj.__version__ === 1) {
                            return 'insert';
                        }
                        else {
                            return 'update';
                        }
                    }

                    async function callSave(obj) {
                        if (obj.__template__ && obj.__template__.__schema__) {
                            return await obj.persistSave(persistorTransaction, logger);
                        }
                        else {
                            return await true;
                        }
                    }
                }

                async function processDeletes() {
                    const deletedArray = _.toArray(deletedObjects);
                    return await process(deletedArray, callDelete, generateChangesAction)

                    function generateChangesAction(obj) {
                        return 'delete';
                    }

                    async function callDelete(obj) {
                        if (obj.__template__ && obj.__template__.__schema__) {
                            return await obj.perssitDelete(persistorTransaction, logger);
                        }
                        else {
                            return true;
                        }
                    }
                }

                async function process(arrayObjects: any[], callBack: (obj) => Promise<any>, generateChangesAction: (obj) => string) {
                    await Promise.all(arrayObjects.map(async (obj: Persistent) => {
                        delete arrayObjects[obj.__id__]; // Once scheduled for update remove it.
                        await callBack(obj);
                        return await generateChanges(obj, generateChangesAction)
                    }));

                    if (arrayObjects.length > 0) {
                        return await process(arrayObjects, callBack, generateChangesAction);
                    }
                }

                async function processDeleteQueries(persistor: typeof PersistObjectTemplate) {
                    const deleteArray = _.toArray(deleteQueries);
                    await Promise.all(deleteArray.map(async (obj: any) => {
                        delete deleteQueries[obj.name];  // Once scheduled for update remove it.
                        if (obj.template && obj.template.__schema__) {
                            return await Database.deleteFromKnexQuery(persistor, obj.template, obj.queryOrChains, persistorTransaction, logger)
                        }
                        else {
                            return true;
                        }
                    }));

                    if (deleteArray.length > 0) {
                        return await processDeleteQueries(persistor);
                    }
                }


                async function processPostSave() {
                    return persistorTransaction.postSave ?
                        await persistorTransaction.postSave(persistorTransaction, logger, changeTracking) :
                        true;
                }

                // And we are done with everything
                async function processCommit() {
                    persistor.dirtyObjects = {};
                    persistor.savedObjects = {};
                    if (persistorTransaction.updateConflict) {
                        throw 'Update Conflict';
                    }
                    return await knexTransaction.commit();
                }

                // Walk through the touched objects
                async function processTouches() {

                    const touchArray = _.toArray(touchObjects);

                    return await Promise.all(touchArray.map(async (obj: Persistent) => {
                        if (obj.__template__ && obj.__template__.__schema__ && !savedObjects[obj.__id__]) {
                            return await obj.persistTouch(persistorTransaction, logger);
                        }
                        else {
                            return true;
                        }
                    }));
                }

                async function rollback(err) {
                    const deadlock = err.toString().match(/deadlock detected$/i);
                    persistorTransaction.innerError = err;
                    innerError = deadlock ? new Error('Update Conflict') : err;
                    await knexTransaction.rollback(innerError);
                    logger.debug({ component: 'persistor', module: 'api', activity: 'end' }, 'transaction rolled back ' +
                        innerError.message + (deadlock ? ' from deadlock' : ''));
                }

                function generateChanges(obj, action) {
                    let objChanges;

                    if (notifyChanges && obj.__template__.__schema__.enableChangeTracking) {
                        changeTracking = changeTracking || {};
                        changeTracking[obj.__template__.__name__] = changeTracking[obj.__template__.__name__] || [];
                        changeTracking[obj.__template__.__name__].push(objChanges = {
                            table: obj.__template__.__table__,
                            primaryKey: obj._id,
                            action: action,
                            properties: []
                        });
                        if (action === 'update' || action === 'delete') {
                            const props = obj.__template__.getProperties();
                            for (const prop in props) {
                                const propType = props[prop];
                                if (isOnetoManyRelationsOrPersistorProps(prop, propType, props)) {
                                    continue;
                                }
                                generatePropertyChanges(prop, obj, props);
                            }
                        }
                    }

                    function isOnetoManyRelationsOrPersistorProps(propName: string, propType, allProps) {
                        return (propType.type === Array && propType.of.isObjectTemplate) ||
                            (propName.match(/Persistor$/) && typeof allProps[propName.replace(/Persistor$/, '')] === 'object');
                    }

                    function generatePropertyChanges(prop, obj, props) {
                        //When the property type is not an object template, need to compare the values.
                        //for date and object types, need to compare the stringified values.
                        const oldKey = '_ct_org_' + prop;
                        if (!props[prop].type.isObjectTemplate && (obj[oldKey] !== obj[prop] || ((props[prop].type === Date || props[prop].type === Object) &&
                            JSON.stringify(obj[oldKey]) !== JSON.stringify(obj[prop])))) {
                            addChanges(prop, obj[oldKey], obj[prop], prop);
                        }
                        //For one to one relations, we need to check the ids associated to the parent record.
                        else if (props[prop].type.isObjectTemplate && obj['_ct_org_' + prop] !== obj[prop + 'Persistor'].id) {
                            addChanges(prop, obj[oldKey], obj[prop + 'Persistor'].id, getColumnName(prop, obj));
                        }
                    }

                    function getColumnName(prop, obj) {
                        const schema = obj.__template__.__schema__;
                        if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id)
                            throw new Error(obj.__template__.__name__ + '.' + prop + ' is missing a parents schema entry');
                        return schema.parents[prop].id;
                    }

                    function addChanges(prop, originalValue, newValue, columnName) {
                        objChanges.properties.push({
                            name: prop,
                            originalValue: originalValue,
                            newValue: newValue,
                            columnName: columnName
                        });
                    }
                }
            })

            logger.debug({ component: 'persistor', module: 'api' }, 'end - transaction completed');
            return true;
        }
        catch (e) {
            const err = e || innerError;
            if (err && err.message && err.message != 'Update Conflict') {
                logger.error({ component: 'persistor', module: 'api', activity: 'end', error: err.message + err.stack }, 'transaction ended with error');
            } //@TODO: Why throw error in all cases but log only in some cases
            throw (e || innerError);
        }
    }

    /**
     * Create a table based on the schema definitions, will consider even indexes creation.
     * @param {object} template super type
     * @param {string} collection collection/table name
     * @returns {*}
     */
    async function _createKnexTable(persistor: typeof PersistObjectTemplate, template, collection) {
        collection = collection || template.__table__;

        while (template.__parent__) {
            template = template.__parent__;
        }

        const knex = UtilityFunctions.getKnexConnection(persistor, template);
        const tableName = UtilityFunctions.dealias(collection);
        return await knex.schema.createTable(tableName, createColumns);

        function createColumns(table) {
            table.string('_id').primary();
            table.string('_template');
            table.biginteger('__version__');
            const columnMap = {};

            recursiveColumnMap(template);

            function mapTableAndIndexes(table, props, schema) {
                for (const prop in props) {
                    if (!columnMap[prop]) {
                        const defineProperty = props[prop];

                        if (!UtilityFunctions._persistProperty(defineProperty))
                            continue;
                        if (defineProperty.type === Array) {
                            if (!defineProperty.of.__objectTemplate__)
                                table.text(prop);
                        } else if (defineProperty.type && defineProperty.type.__objectTemplate__) {
                            if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id) {
                                throw new Error(template.__name__ + '.' + prop + ' is missing a parents schema entry');
                            }

                            const foreignKey = (schema.parents && schema.parents[prop]) ? schema.parents[prop].id : prop;
                            table.text(foreignKey);
                        } else if (defineProperty.type === Number) {
                            table.double(prop);
                        } else if (defineProperty.type === Date) {
                            table.timestamp(prop);
                        } else if (defineProperty.type === Boolean) {
                            table.boolean(prop);
                        } else {
                            table.text(prop);
                        }

                        columnMap[prop] = true;
                    }
                }
            }

            function recursiveColumnMap(childTemplate) {
                if (childTemplate) {
                    mapTableAndIndexes(table, childTemplate.defineProperties, childTemplate.__schema__);
                    childTemplate = childTemplate.__children__;
                    childTemplate.forEach((o) => {
                        recursiveColumnMap(o);
                    });
                }
            }
        }
    }

    export function convertMongoQueryToChains(alias, statement, query) {

        /**
         * Traverse an object and produce a promise chain of where and andWhere
         * @param {object} statement knex query
         * @param {object} query mongo style query object
         * @returns {*}
         */
        function traverse(statement, query) {
            var firstProp = true;
            for (var prop in query) {
                var params = processProp(statement, prop, query[prop]);
                statement = firstProp ?
                    (params.length > 1 ? statement.where(params[0], params[1], params[2]) :
                        statement.where(params[0])) :
                    (params.length > 1 ? statement.andWhere(params[0], params[1], params[2]) :
                        statement.andWhere(params[0]));
                firstProp = false;
            }
            return statement;
        }

        function processProp(_statement, prop, value) {
            if (value instanceof Array)
                return processArrayProp(prop, value);
            else
                return processNonArrayProp(prop, value)
        }

        function processArrayProp(prop, value) {
            return [function () {
                var firstProp;
                var statement = this;
                if (prop.toLowerCase() == '$and') {
                    firstProp = true;
                    _.each(value, function (obj) {
                        var params = processObject(statement, obj);
                        statement = firstProp ?
                            (params.length > 1 ? statement.where(params[0], params[1], params[2]) :
                                statement.where(params[0])) :
                            (params.length > 1 ? statement.andWhere(params[0], params[1], params[2]) :
                                statement.andWhere(params[0]));
                        firstProp = false;
                    });
                } else if (prop.toLowerCase() == '$or') {
                    firstProp = true;
                    _.each(value, function (obj) {
                        var params = processObject(statement, obj);
                        statement = firstProp ?
                            (params.length > 1 ? statement.where(params[0], params[1], params[2]) :
                                statement.where(params[0])) :
                            (params.length > 1 ? statement.orWhere(params[0], params[1], params[2]) :
                                statement.andWhere(params[0]));
                        firstProp = false
                    });
                } else if (prop.toLowerCase() == '$in')
                    statement = statement.whereIn(value);
                else if (prop.toLowerCase() == '$nin')
                    statement = statement.whereNotIn(value);
                else
                    throw 'Don\'t support ' + prop + ':' + JSON.stringify(value)
            }];
        }

        /**
         * Process an array element of a $or or $and.  This will result in either three parameters in
         * the form of prop, compare operator, value or a single parameter which is a function that
         * will chain together a nested expression.
         * @param {object} statement knex query object
         * @param {object} obj object of supertype
         * @returns {Function}
         */
        function processObject(statement, obj) {
            var propCount = 0;
            var singleProp;

            // Do we have more than one prop
            for (var prop in obj) {
                singleProp = prop;
                ++propCount;
            }

            // If so fetch the 3 parameters for a where, orWhere or andWhere chain
            // Otherwise return a function that will chain sub-ordinate clauses
            if (propCount == 1)
                return processProp(statement, singleProp, obj[singleProp]);
            else
                return [function () {
                    traverse(statement, obj)
                }]
        }

        function processNonArrayProp(prop, value) {
            let params = [];
            if (value instanceof Date || typeof (value) == 'string' || typeof (value) == 'number') {
                params[0] = alias + '.' + prop;
                params[1] = '=';
                params[2] = value;
            } else
                for (var subProp in value) {
                    params[0] = alias + '.' + prop;
                    params[2] = value[subProp];
                    if (subProp.toLowerCase() == '$eq')
                        params[1] = '=';
                    else if (subProp.toLowerCase() == '$gt')
                        params[1] = '>';
                    else if (subProp.toLowerCase() == '$gte')
                        params[1] = '>=';
                    else if (subProp.toLowerCase() == '$lt')
                        params[1] = '<';
                    else if (subProp.toLowerCase() == '$lte')
                        params[1] = '<=';
                    else if (subProp.toLowerCase() == '$ne')
                        params[1] = '!=';
                    else if (subProp.toLowerCase() == '$in') {
                        {
                            let attr = params[0];
                            let values = params[2];
                            params = [function () {
                                this.whereIn(attr, values)
                            }];
                        }
                    }
                    else if (subProp.toLowerCase() == '$nin') {
                        {
                            let attr = params[0];
                            let values = params[2];
                            params = [function () {
                                this.whereNotIn(attr, values)
                            }];
                        }
                    }
                    else if (subProp.toLowerCase() == '$regex') {
                        params[1] = value.$options && value.$options.match(/i/) ? '~*' : '~';
                        delete value['$options']
                        if (params[2] && params[2].source)
                            params[2] = params[2].source;
                    } else
                        throw 'Can\'t handle ' + prop + ':' + JSON.stringify((value));
                }
            return params;
        }

        return traverse(statement, query);
    }

    function executeQueryOrChains(queryOrChains, select, tableName) {
        //  execute callback to chain on filter functions or convert mongo style filters
        if (queryOrChains) {
            if (typeof (queryOrChains) == 'function') {
                queryOrChains(select);
            }
            else if (queryOrChains) {
                return convertMongoQueryToChains(tableName, select, queryOrChains);
            }
        }
        return select;
    }
}