import * as _ from 'underscore';

import { ObjectTemplate } from 'supertype';

import { MongoClient } from 'mongodb';
import { db as DefaultDB } from './index';

import * as knex from 'knex';
import { PersistentConstructor } from './Persistent';
import { Schema } from './Schema';
import { SchemaValidator } from './SchemaValidator';
import { UtilityFunctions } from './UtilityFunctions';
import { Mongo } from './Mongo';
type Transaction = {
    id: number,
    dirtyObjects: Object,
    savedObjects: Object,
    touchObjects: Object,
    deletedObjects: Object,
    deleteQueries?: Object
}

type KnexOrMongo = 'knex' | 'mongo';
// @TODO: Need to export ObjectTemplate as well

// This is kinda jank because it's not REALLY extending OT. This should really be a mixin.
// But typescript mixins are bad. 
// Actually I'm not sure if it should be a mixin even.

export class PersistObjectTemplate extends ObjectTemplate {

    static DB_Knex = 'knex';
    static DB_Mongo = 'mongo';
    static schemaVerified: boolean;
    static baseClassForPersist: typeof ObjectTemplate;
    static currentTransaction: Transaction;
    static dirtyObjects: any;
    static savedObjects: {};

    // instance of Knex
    static _db: {[key: string]: {connection: knex | MongoClient, type: KnexOrMongo}};
    static __defaultTransaction__: Transaction;
    static _schema: any;
    static noAutoIndex: any;
    static __dictionary__: {[key: string]: PersistentConstructor};

    static _id: any;
    static deletedObjects: any;
    static __transient__: boolean;
    static objectMap: boolean;
    static __changeTracking__: boolean;

    // @TODO: remove for uuid
    // static objId = ObjectID;

    // @TODO: Does this even need to be passed in? Can we just use the reference to ObjectTemplate here
    static initialize(baseClassForPersist: typeof ObjectTemplate) {
        this.init();
        this.baseClassForPersist = baseClassForPersist;
        return this;
    }

    /**
     * 
     *  REDIRECTS FOR SCHEMA
     */

    static setSchema(schema) {
        return Schema.setSchema(this, schema);
    }

    static appendSchema(schema) {
        return Schema.appendSchema(this, schema);
    }

    /**
    * Run through the schema entries and setup these properites on templates
    *  __schema__: the schema for each template
    *  __collection__: the name of the Mongo Collection
    *  __topTemplate__: for a template that represents a subDocument the template that is primary for that colleciton
    * 
    * @private
    */
    static _verifySchema() {
        return Schema._verifySchema(this);
    }

    static isCrossDocRef(template, prop, defineProperty) {
        return Schema.isCrossDocRef(this, template, prop, defineProperty);
    }


    /**
     *  API.TS
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
    static _injectIntoTemplate(template) {
        this._prepareSchema(template);

        // Add persistors to foreign key references
        if (template.defineProperties && typeof (template._injectProperties) == 'function')
            template._injectProperties();
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

    /**
     * PUBLIC INTERFACE FOR objectTemplate
     */

    /**
    * Begin a transaction that will ultimately be ended with end. It is passed into setDirty so
    * dirty objects can be accumulated.  Does not actually start a knex transaction until end
    * @param {bool} notDefault used for marking the transaction created as the default transaction
    * @returns {object} returns transaction object
    */
    static begin(notDefault?: boolean): Transaction {
        const txn: Transaction = {
            id: new Date().getTime(),
            dirtyObjects: {},
            savedObjects: {},
            touchObjects: {},
            deletedObjects: {}
        };
        if (!notDefault) {
            this.currentTransaction = txn;
        }
        return txn;
    }

    static end(persistorTransaction?, logger?) {
        const txn = persistorTransaction || this.currentTransaction;
        const usedLogger = logger || this.logger;
        return this.commit({
            transaction: txn,
            logger: usedLogger
        });
    }

    /**
     * Set the object dirty along with all descendant objects in the logical "document"
     *
     * @param {supertype} obj objecttempate
     * @param {object} txn persistobjecttemplate transaction object
     * @param {bool} onlyIfChanged mark dirty only if changed
     * @param {bool} noCascade, avoids loading children
     * @param {object} logger objecttemplate logger
     */
    static setDirty(obj, txn?, onlyIfChanged?, noCascade?, logger?) {
        var topObject;

        const usedLogger = logger || this.logger;
        // Get array references too
        if (onlyIfChanged && this.MarkChangedArrayReferences) {
            this.MarkChangedArrayReferences();
        }

        txn = txn || this.currentTransaction;

        if (!obj || !obj.__template__.__schema__) {
            return;
        }

        // Use the current transaction if none passed
        txn = txn || PersistObjectTemplate.currentTransaction || null;

        if (!onlyIfChanged || obj.__changed__) {
            (txn ? txn.dirtyObjects : this.dirtyObjects)[obj.__id__] = obj;
        }

        if (txn && obj.__template__.__schema__.cascadeSave && !noCascade) {

            // Potentially cascade to set other related objects as dirty
            topObject = PersistObjectTemplate.getTopObject(obj);

            if (!topObject) {
                usedLogger.error(
                    {
                        component: 'persistor',
                        module: 'api',
                        activity: 'setDirty'
                    }, `Warning: setDirty called for ${obj.__id__} which is an orphan`);
            }

            if (topObject && topObject.__template__.__schema__.cascadeSave) {

                const newTopObject = PersistObjectTemplate.getTopObject(obj);

                PersistObjectTemplate.enumerateDocumentObjects(newTopObject, (obj) => {
                    if (!onlyIfChanged || obj.__changed__) {
                        (txn ? txn.dirtyObjects : this.dirtyObjects)[obj.__id__] = obj;
                        // Touch the top object if required so that if it will be modified and can be refereshed if needed
                        if (txn && txn.touchTop && obj.__template__.__schema__) {
                            let topObject = PersistObjectTemplate.getTopObject(obj);
                            if (topObject) {
                                txn.touchObjects[topObject.__id__] = topObject;
                            }
                        }
                    }
                });
            }
        }

        if (txn && txn.touchTop && obj.__template__.__schema__) {
            topObject = PersistObjectTemplate.getTopObject(obj);
            if (topObject) {
                txn.touchObjects[topObject.__id__] = topObject;
            }
        }
    }

    static setAsDeleted(obj, txn?, onlyIfChanged?, ...args) {
        // Get array references too
        if (onlyIfChanged && this.MarkChangedArrayReferences) {
            this.MarkChangedArrayReferences();
        }

        txn = txn || this.__defaultTransaction__;

        if (!obj || !obj.__template__.__schema__) {
            return;
        }

        if (!onlyIfChanged || obj.__deleted__) {
            (txn ? txn.deletedObjects : this.deletedObjects)[obj.__id__] = obj;
        }
        //Do we need to support cascase delete, if so we need to check the dependencies and delete them.    
    }

    // @TODO: need to test that this works
    static async saveAll(txn, logger?) {
        let somethingSaved = false;
        const dirtyObjects = txn ? txn.dirtyObjects : this.dirtyObjects;
        
        const results = Object.keys(dirtyObjects).map(async (key, index)=> {
            let obj = dirtyObjects[key];
            delete dirtyObjects[obj.__id__];
            await obj.persistSave(txn, logger);
            this.saved(obj, txn);
            somethingSaved = true;
        });

        await Promise.all(results);
        
        if (!somethingSaved && txn && txn.postSave) {
            txn.postSave(txn, logger);
            this.dirtyObjects = {};
            this.savedObjects = {};
        }

        if (somethingSaved) {
            return this.saveAll(txn);
        }
        else {
            return true;
        }
    }

    /**
    * Set a data base to be used
    * @param {knex|mongoclient} db - the native client objects used
    * @param {knex|mongo} type - the type which is defined in index.js
    * @param {pg|mongo|__default} alias - An alias that can be used in the schema to specify the database at a table level
    */
    static setDB (db, type, alias) {
        type = type || PersistObjectTemplate.DB_Mongo;
        alias = alias || '__default__';
        this._db = this._db || {};
        this._db[alias] = {connection: db || DefaultDB, type: type}
    }
    
    /**
     * retrieve a PLain Old Javascript Object given a query
     * @param {SuperType} template - template to load
     * @param {json|function} query - can pass either mongo style queries or callbacks to add knex calls..
     * @param {json} options - sort, limit, and offset options
     * @param {ObjectTemplate.logger} logger - objecttemplate logger
     * @returns {*}
     * 
     * @TODO: remove circular ref to this
     * */
    static async getPOJOFromQuery  (template, query, options?, logger?) {
        
        const prefix = this.dealias(template.__collection__);
        
        let pojos;
        if (UtilityFunctions.isDBMongo(this, __template__.__collection__)) {
            pojos = await Mongo.getPOJOByQuery(this, template, query, options, logger);
        }
        else {
            pojos = await this.getPOJOsFromKnexQuery(template, [], query, options, undefined, logger);
        }
        // @TODO make sure this is supposed to happen for both mongo and knex ask srksag
        pojos.forEach((pojo) => {
            _.map(pojo, (_val, prop: string) => {
                if (prop.match(RegExp('^' + prefix + '___'))) {
                    pojo[prop.replace(RegExp('^' + prefix + '___'), '')] = pojo[prop];
                    delete pojo[prop];
                }
            });
        });

        return pojos;
    }

    static beginTransaction(): Transaction {
        var txn = {
            id: new Date().getTime(),
            dirtyObjects: {},
            savedObjects: {},
            touchObjects: {},
            deletedObjects: {},
            deleteQueries: {}
        };
        return txn;
    }

    static beginDefaultTransaction() {
        this.__defaultTransaction__ = {
            id: new Date().getTime(),
            dirtyObjects: {},
            savedObjects: {},
            touchObjects: {},
            deletedObjects: {}
        };

        return this.__defaultTransaction__;
    }

    static async commit(options?) {
        SchemaValidator.validate(options, 'commitSchema');

        options = options || {};
        var logger = options.logger || PersistObjectTemplate.logger;

        const persistorTransaction: Transaction = options.transaction || this.__defaultTransaction__;

        if (PersistObjectTemplate.DB_Knex) {
            return await PersistObjectTemplate._commitKnex(persistorTransaction, logger, options.notifyChanges);
        }
    }

    /**
     * 
     *  UNIT TESTING Persistor assisting methods.
     * 
     */
    
    
    /**
     * Mostly used for unit testing.
     * Does a knex connect, schema setup and injects templates
     * @param {object} config knex connection
     * @param {JSON} schema data model definitions
     * @returns {*}
     */
    static connect (config, schema) {
        var connection = knex(config);
        this.setDB(connection, this.DB_Knex,  config.client);
        this.setSchema(schema);
        this.performInjections(); // Normally done by getTemplates
        return connection;
    }

    /**
     * Mostly used for unit testing.  Drops all tables for templates that have a schema
     * @returns {*|Array}
     */
    
     static async dropAllTables() {
        let results = await this.onAllTables(async (template) => await this.synchronizeKnexTableFromTemplate(template));
        return await Promise.all(results);
    }

    /**
    * Mostly used for unit testing.  Synchronize all tables for templates that have a schema
    * @returns {*|Array}
    */
    static async syncAllTables() {
        let results = await this.onAllTables(async (template) => await this.synchronizeKnexTableFromTemplate(template));
        return await Promise.all(results);
    }
    /**
     * Mostly used for unit testing.  Synchronize all tables for templates that have a schema
     * @param {string} action common actions
     * @param {string} concurrency #parallel
     * @returns {*|Array}
     */
    static async onAllTables (action: (template) => Promise<any>): Promise<any[]> {
        var templates = [];
        _.each(this.__dictionary__, (template) => {
            if (template.__schema__ && (!template.__schema__.documentOf || !template.__schema__.documentOf.match(/not persistent/i))) {
                templates.push(template);
            }
        });

        return await Promise.all(templates.map(action));
    }

}