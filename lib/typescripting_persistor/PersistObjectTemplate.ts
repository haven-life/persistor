import * as _ from 'underscore';

import {ObjectTemplate} from 'supertype';

// @TODO: Need to export ObjectTemplate as well

// This is kinda jank because it's not REALLY extending OT. This should really be a mixin.
// But typescript mixins are bad. 
// Actually I'm not sure if it should be a mixin even.

export class PersistObjectTemplate extends ObjectTemplate {

    static DB_Knex = 'knex';
    static DB_Mongo = 'mongo';
    static schemaVerified: boolean;

    static baseClassForPersist: typeof ObjectTemplate;

    static initialize(baseClassForPersist: typeof ObjectTemplate) {
        this.init();
        this.baseClassForPersist = baseClassForPersist;
    }

    
    /**
     * 
     * 
     *  API . TS
     * 
     * 
     */

    /**
    * PUBLIC INTERFACE FOR OBJECTS
    */
    static getPersistorProps() {
        let persistorProps = {};

        Object.keys(this.__dictionary__).forEach((key, index) => {
            let template = this.__dictionary__[key];

            let props = template.getProperties(false); // @TODO: Need to change typing to make includeVirtual optional

            Object.keys(props).forEach(prop => {
                if (prop.match(/Persistor$/) && prop.substr(0, 2) != '__') {
                    persistorProps[template.__name__] = persistorProps[template.__name__] || {}
                    persistorProps[template.__name__][prop.replace(/Persistor$/, '')] = 1;
                }
            });
        });    

        return persistorProps;
    }
    
    /**
     * PUBLIC INTERFACE FOR TEMPLATES
     *
     * @param {supertype} template - load all parent/child/subdocument/subsetof defitions
     */
    static _injectIntoTemplate (template) {
        this._prepareSchema(template);
        this._injectTemplateFunctions(template);
        this._injectObjectFunctions(template);
    }

    static _prepareSchema(template) {
        if (!this.schemaVerified) {
            this._verifySchema();
        }
        this.schemaVerified = true;

        // Process subclasses that didn't have schema entries
        var parent = template.__parent__;
        while (!template.__schema__ && parent) {
            if (parent.__schema__) {
                template.__schema__ = parent.__schema__;
                template.__collection__ = parent.__collection__;
                template.__table__ = template.__schema__.table ? template.__schema__.table : parent.__table__;
                template.__topTemplate = parent.__topTemplate__;
                parent = null;
            } else {
                parent = parent.__parent__;
            }
        }

        // Process subsets
        if (template.__schema__ && template.__schema__.subsetOf) {
            var mainTemplate = this.__dictionary__[template.__schema__.subsetOf];
            if (!mainTemplate) {
                throw new Error('Reference to subsetOf ' + template.__schema__.subsetOf + ' not found for ' + template.__name__);
            }
            template.__subsetOf__ = template.__schema__.subsetOf
            if (!mainTemplate.__schema__) {
                parent = mainTemplate.__parent__;
                while (!mainTemplate.__schema__ && parent) {
                    if (parent.__schema__) {
                        mainTemplate.__schema__ = parent.__schema__;
                        mainTemplate.__collection__ = parent.__collection__;
                        mainTemplate.__table__ = mainTemplate.__schema__.table ? mainTemplate.__schema__.table : parent.__table__;
                        mainTemplate.__topTemplate = parent.__topTemplate__;
                        parent = null;
                    } else {
                        parent = parent.__parent__;
                    }
                }
                if (!mainTemplate.__schema__) {
                    throw new Error('Missing schema entry for ' + template.__schema__.subsetOf);
                }
            }
            mergeRelationships(template.__schema__, mainTemplate.__schema__);
            template.__collection__ = mainTemplate.__collection__;
            template.__table__ = mainTemplate.__table__;
        }
        this.baseClassForPersist._injectIntoTemplate(template);

        function mergeRelationships(orig, overlay) {
            _.each(overlay.children, function (value, key) {
                orig.children = orig.children || {};
                if (!orig.children[key]) {
                    orig.children[key] = value;
                }
            });
            _.each(overlay.parents, function (value, key) {
                orig.parents = orig.parents || {};
                if (!orig.parents[key]) {
                    orig.parents[key] = value;
                }
            });
        }
    }

    static _injectTemplateFunctions(template) {


        /**
         * Fetch all objects matching a query
         * @param {JSON} query @TODO
         * @param {JSON} options @TODO
         * @returns {*}
         */
        template.persistorFetchByQuery = async function(query, options) {
            PersistObjectTemplate._validateParams(options, 'fetchSchema', template);

            options = options || {};
            var persistObjectTemplate = options.session || PersistObjectTemplate;
            var logger = options.logger || persistObjectTemplate.logger;
            logger.debug({component: 'persistor', module: 'api', activity: 'getFromPersistWithQuery',
                data: {template: template.__name__}});
            var dbType = persistObjectTemplate.getDB(persistObjectTemplate.getDBAlias(template.__collection__)).type;
            if (options.order && !options.order.sort) {
                options.order = { sort: options.order };
            }
            let fetchQuery = (dbType == persistObjectTemplate.DB_Mongo ?
                persistObjectTemplate.getFromPersistWithMongoQuery(template, query, options.fetch, options.start,
                            options.limit, options.transient, options.order, options.order, logger) :
                persistObjectTemplate.getFromPersistWithKnexQuery(null, template, query, options.fetch, options.start,
                            options.limit, options.transient, null, options.order,
                            undefined, undefined, logger, options.enableChangeTracking, options.projection));
            return fetchQuery.catch(e => logExceptionAndRethrow(e, options.logger || PersistObjectTemplate.logger, template.__name__, query, 'persistorFetchByQuery'));
        };
        /**
         * Return count of objects of this class given a json query
         *
         * @param {json} query mongo style queries
         * @param {object} options @TODO
         * @returns {Number}
         */
        template.persistorCountByQuery = async function(query, options) {
            PersistObjectTemplate._validateParams(options, 'fetchSchema', template);

            options = options || {};
            var logger = options.logger || PersistObjectTemplate.logger;
            logger.debug({component: 'persistor', module: 'api', activity: 'getFromPersistWithQuery',
                data: {template: template.__name__}});

            var dbType = PersistObjectTemplate.getDB(PersistObjectTemplate.getDBAlias(template.__collection__)).type;
            let countQuery = (dbType == PersistObjectTemplate.DB_Mongo ?
                PersistObjectTemplate.countFromMongoQuery(template, query, logger) :
                PersistObjectTemplate.countFromKnexQuery(template, query, logger))
                .then(function(res) {
                    return res;
                }.bind(this));
            return countQuery.catch(e => logExceptionAndRethrow(e, options.logger || PersistObjectTemplate.logger, template.__name__, query, {activity: 'persistorCountByQuery'}));
        };

        /**
         * Delete objects given a json query
         *
         * @param {string} id mongo style id
         * @param {object} txn persistObjectTemplate transaciton object
         * @param {object} logger objecttemplate logger
         * @returns {object}
         * @deprecated in favor of persistorDeleteByQuery
         */
        template.deleteFromPersistWithId = async function(id, txn, logger) {
            (logger || PersistObjectTemplate.logger).debug({component: 'persistor', module: 'api', activity: 'deleteFromPersistWithId',
                data: {template: template.__name__, id: id}});
            var dbType = PersistObjectTemplate.getDB(PersistObjectTemplate.getDBAlias(template.__collection__)).type;
            let deleteQuery = (dbType == PersistObjectTemplate.DB_Mongo ?
                PersistObjectTemplate.deleteFromPersistWithMongoId(template, id, logger) :
                PersistObjectTemplate.deleteFromKnexId(template, id, txn, logger))
                .then(function(res) {
                    return res;
                }.bind(this));
            return deleteQuery.catch(e => logExceptionAndRethrow(e, logger || PersistObjectTemplate.logger, template.__name__, id, {activity: 'deleteFromPersistWithId'}));
        };

        /**
         * Return count of objects of this class given a json query
         *
         * @param {json} query mongo style queries
         * @param {object} logger objecttemplate logger
         * @returns {Number}
         * @deprecated in favor of persistorCountWithQuery
         */
        template.countFromPersistWithQuery = async function(query, logger) {
            (logger || PersistObjectTemplate.logger).debug({component: 'persistor', module: 'api', activity: 'countFromPersistWithQuery',
                data: {template: template.__name__}});
            var dbType = PersistObjectTemplate.getDB(PersistObjectTemplate.getDBAlias(template.__collection__)).type;
            let countQuery = (dbType == PersistObjectTemplate.DB_Mongo ?
                PersistObjectTemplate.countFromMongoQuery(template, query, logger) :
                PersistObjectTemplate.countFromKnexQuery(template, query, logger))
                .then(function(res) {
                    return res;
                }.bind(this));
            return countQuery.catch(e => logExceptionAndRethrow(e, logger || PersistObjectTemplate.logger, template.__name__, query, 'countFromPersistWithQuery'));
        };

        /**
         * Determine whether we are using knex on this table
         * @returns {boolean}
         */
        template.persistorIsKnex = function () {
            var dbType = PersistObjectTemplate.getDB(PersistObjectTemplate.getDBAlias(template.__collection__)).type;
            return dbType != PersistObjectTemplate.DB_Mongo;
        };

        /**
         * Get a knex object that can be used to create native queries (e.g. template.getKnex().select().from())
         * @returns {*}
         */
        template.persistorGetKnex = function () {
            var tableName = PersistObjectTemplate.dealias(template.__table__);
            return PersistObjectTemplate.getDB(PersistObjectTemplate.getDBAlias(template.__table__)).connection(tableName);
        };

        /**
         * Return knex table name for template for use in native queries
         * @param {string} alias - table alias alias named used when setting the DB object
         * @returns {string}
         */
        template.persistorGetTableName = function (alias) {
            return PersistObjectTemplate.dealias(template.__table__) + (alias ? ' as ' + alias : '');
        };

        /**
         * Return the foreign key for a given parent property for use in native queries
         * @param {string} prop field name
         * @param {string} alias - table alias name used for query generation
         * @returns {string}
         */
        template.persistorGetParentKey = function (prop, alias) {
            return (alias ? alias + '.'  : '') + template.__schema__.parents[prop].id;
        };

        /**
         * Return the foreign key for a given child property for use in native queries
         * @param {string} prop field name
         * @param {string} alias - table alias name used for query generation
         * @returns {string}
         */
        template.persistorGetChildKey = function (prop, alias) {
            return (alias ? alias + '.'  : '') + template.__schema__.children[prop].id;
        };

        /**
         * Return '_id'
         * @param {string} alias - table alias name used for query generation
         * @returns {string}
         */
        template.persistorGetId = function (alias) {
            return (alias ? alias + '.'  : '') + '_id';
        };

        /**
         * return an array of join parameters (e.g. .rightOuterJoin.apply(template.getKnex(), Transaction.knexChildJoin(...)))
         * @param {object} targetTemplate objecttemplate
         * @param {string} primaryAlias - table alias name used for query generation
         * @param {string} targetAlias - table alias name used for query generation
         * @param {string} joinKey - field name
         * @returns {*[]}
         */
        template.persistorKnexParentJoin = function (targetTemplate, primaryAlias, targetAlias, joinKey) {
            return [template.getTableName() + ' as ' + primaryAlias, targetTemplate.getParentKey(joinKey, targetAlias), template.getPrimaryKey(primaryAlias)];
        };

        /**
         * return an array of join parameters (e.g. .rightOuterJoin.apply(template.getKnex(), Transaction.knexChildJoin(...)))
         * @param {object} targetTemplate target table to join with
         * @param {object} primaryAlias table alias name for the source/current object
         * @param {object} targetAlias table alias name for the target table.
         * @param {string} joinKey source table field name
         * @returns {*[]}
         */
        template.persistorKnexChildJoin = function (targetTemplate, primaryAlias, targetAlias, joinKey) {
            return [template.getTableName() + ' as ' + primaryAlias, targetTemplate.getChildKey(joinKey, primaryAlias), targetTemplate.getPrimaryKey(targetAlias)];
        };


        // Deprecated API
        template.isKnex = template.persistorIsKnex;
        template.getKnex = template.persistorGetKnex;
        template.getTableName = template.persistorGetTableName;
        template.getParentKey = template.persistorGetParentKey;
        template.getChildKey = template.persistorGetChildKey;
        template.getPrimaryKey = template.persistorGetId;
        template.knexParentJoin = template.persistorKnexParentJoin;
        template.knexChildJoin = template.persistorKnexChildJoin;

        /**
         * Inject the persitor properties and get/fetch methods need for persistence.  This is either called
         * as part of _injectTemplate if the template was fully created or when the template is instantiated lazily
         * @private
         */
        template._injectProperties = function () {
            if (this.hasOwnProperty('__propertiesInjected__'))
                return;
            var props = this.defineProperties;
            for (var prop in props) {
                var defineProperty = props[prop];
                var type = defineProperty.type;
                var of = defineProperty.of;
                var refType = of || type;

                if (refType && refType.isObjectTemplate && PersistObjectTemplate._persistProperty(defineProperty)) {
                    var isCrossDocRef = PersistObjectTemplate.isCrossDocRef(template, prop, defineProperty)
                    if (isCrossDocRef || defineProperty.autoFetch) {
                        (function () {
                            var closureProp = prop;
                            var closureFetch = defineProperty.fetch ? defineProperty.fetch : {};
                            var closureQueryOptions = defineProperty.queryOptions ? defineProperty.queryOptions : {};
                            var toClient = !(defineProperty.isLocal || (defineProperty.toClient === false))
                            if (!props[closureProp + 'Persistor']) {
                                template.createProperty(closureProp + 'Persistor', {type: Object, toClient: toClient,
                                    toServer: false, persist: false,
                                    value: {isFetched: defineProperty.autoFetch ? false : true, isFetching: false}});
                            }
                            if (!template.prototype[closureProp + 'Fetch'])
                                template.createProperty(closureProp + 'Fetch', {on: 'server', body: function (start, limit) {
                                    if (typeof(start) != 'undefined') {
                                        closureQueryOptions['skip'] = start;
                                    }
                                    if (typeof(limit) != 'undefined') {
                                        closureQueryOptions['limit'] = limit;
                                    }
                                    return this.fetchProperty(closureProp, closureFetch, closureQueryOptions);
                                }});
                        })();
                    }
                }
            }
            this.__propertiesInjected__ = true;
        }
    }

}