import { PersistObjectTemplate } from "./PersistObjectTemplate";
import { UtilityFunctions } from "./UtilityFunctions";
import { ObjectID } from 'mongodb';
import { Schema } from "./Schema";

export namespace Mongo {

    /**
     *  savePojoToMongo
     */
    /* Mongo implementation of save */
    export async function save(persistor: typeof PersistObjectTemplate, obj, pojo, updateID, _txn, logger) {
        const usedLogger = logger || persistor.logger;
        usedLogger.debug({
            component: 'persistor',
            module: 'db',
            activity: 'write'
        },
            `Saving ${obj.__template__.__name__} to ${obj.__template__.__collection__}`);

        const origVer = obj.__version__;

        obj.__version__ = obj.__version__ ? obj.__version__ + 1 : 1;
        pojo.__version__ = obj.__version__;

        const collection = UtilityFunctions.getCollectionByObject(persistor, obj);

        if (updateID) {
            let blob = {};
            if (origVer) {
                blob = { __version__: origVer, _id: updateID };
            }
            else {
                blob = { _id: updateID };
            }

            return await collection.update(blob);
        }
        else {
            return await collection.save(pojo, { w: 1 });
        }
    }

    /**
     * Removes documents based on a query
     * @param {SuperType} template object to delete
     * @param {json} query mongo style queries
     * @param {object} _logger objecttemplate logger
     * @returns {object} commandresult of mongo client
     */

    // deleteFromMongoQuery
    export async function deleteQuery(persistor: typeof PersistObjectTemplate, template, query, _logger) {
        const collection = UtilityFunctions.getCollectionByTemplate(persistor, template)
        return await collection.remove(query, { w: 1, fsync: true });
    }


    // getPOJOFromMongoQuery
    export async function getPOJOByQuery(persistor: typeof PersistObjectTemplate, template, query, options, logger?) {
        const usedLogger = logger || persistor.logger;

        usedLogger.debug({
            component: 'persistor',
            module: 'db',
            activity: 'read'
        }, `db.${template.__collection__}.find({${JSON.stringify(query)}})'`);

        const collection = UtilityFunctions.getCollectionByTemplate(persistor, template)

        let newOptions = options || {};

        if (!newOptions.sort) {
            newOptions.sort = { _id: 1 };
        }

        return await collection.find(query, null, newOptions);
    }

    // countFromMongoQuery
    export async function countByQuery(persistor: typeof PersistObjectTemplate, template, query) {
        const collection = UtilityFunctions.getCollectionByTemplate(persistor, template);

        return await collection.count(query);
    }

    // distinctFromMongoQuery
    export async function distinctByQuery(persistor: typeof PersistObjectTemplate, template, field, query) {
        const collection = UtilityFunctions.getCollectionByTemplate(persistor, template);

        return await collection._collection.distinct(field, query);
    }

    // getPOJOFromMongoId
    export async function getPOJOById(persistor: typeof PersistObjectTemplate, template, id, _cascade, _isTransient, idMap) {
        idMap = idMap || {};

        const query = { _id: new ObjectID(id) };

        const pojos = await getPOJOByQuery(persistor, template, query, idMap);

        if (pojos.length > 0) {
            return pojos[0];
        }
        else {
            return null;
        }
    }


    /**
     * Save the object to persistent storage
     *
     * A copy of the object is made which has only the persistent properties
     * and all objects references for objects not stored in the the document
     * replaced by foreign keys.  Arrays of objects not stored in the document
     * are adjusted such that their foreign keys point back to this object.
     * Any related objects stored in other documents are also saved.
     *
     * @param {Supertype} obj  Only required parameter - the object to be saved
     * @param {promises} promises accumulate promises for nested save
     * @param {string} masterId - if we are here to save sub-documents this is the top level id
     * @param {Array} idMap - already loaded objects are being cached
     * @param {Object} txn - uses persistobjecttemplate properties
     * @param {Object} logger = objecttemplate logger
     * @returns {POJO}
     */
    export async function persistSave(persistor: typeof PersistObjectTemplate, obj, promises, masterId, idMap, txn, logger?) {
        if (!obj.__template__) {
            throw new Error('Attempt to save an non-templated Object');
        }
        if (!obj.__template__.__schema__) {
            throw new Error(`Schema entry missing for ${obj.__template__.__name__}`);
        }

        let schema = obj.__template__.__schema__;

        const usedLogger = logger || persistor.logger;
        // Trying to save other than top document work your way to the top
        if (!schema.documentOf && !masterId) {
            const originalObj = obj;


            usedLogger.debug({
                component: 'persistor',
                module: 'update.persistSaveMongo',
                activity: 'save'
            },
                `Search for top of ${obj.__template__.__name__}`);

            obj = UtilityFunctions.getTopObject(persistor, obj);
            if (!obj) {
                throw new Error(`Attempt to save ${originalObj.__template__.__name__} which subDocument without necessary parent links to reach top level document`);
            }

            schema = obj.__template__.__schema__;
            usedLogger.debug({
                component: 'persistor',
                module: 'update.persistSaveMongo',
                activity: 'processing'
            },
                `Found top as ${obj.__template__.__name__}`);
        }

        const collection = obj.__template__.__collection__;
        let resolvePromises = false;    // whether we resolve all promises
        let savePOJO = false;           // whether we save this entity or just return pojo

        if (!promises) {                // accumulate promises for nested saves
            promises = [];
            resolvePromises = true;
        }

        if (typeof (obj._id) == 'function') {
            var followUp = obj._id;
            obj._id = undefined;
        }

        var isDocumentUpdate = obj._id && typeof (masterId) == 'undefined';

        let id;

        if (obj._id) {
            if (obj._id.toString().match(/:/)) {
                id = obj._id;
            }
            else {
                if (obj._id instanceof ObjectID) {
                    id = obj._id;
                }
                else {
                    id = new ObjectID(obj._id);
                }
            }
        }
        else {
            id = UtilityFunctions.getDBID(masterId);
        }

        obj._id = id.toString();
        obj.__dirty__ = false;

        if (followUp)
            followUp.call(null, obj._id);

        if (!masterId) {
            savePOJO = true;

            if (typeof (masterId) == 'undefined') {
                idMap = {};             // Track circular references
            }

            masterId = id;
        }

        // Eliminate circular references
        if (idMap[id.toString()]) {
            usedLogger.debug(
                {
                    component: 'persistor',
                    module: 'update.persistSaveMongo',
                    activity: 'processing'
                },
                'Duplicate processing of ' + obj.__template__.__name__ + ':' + id.toString());
            return idMap[id.toString()];
        }

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'update.persistSaveMongo',
                activity: 'save'
            },
            'Saving ' + obj.__template__.__name__ + ':' + id.toString() + ' master_id=' + masterId);

        var pojo = !isDocumentUpdate ? { _id: id, _template: obj.__template__.__name__ } :
            { _template: obj.__template__.__name__ };   // subsequent levels return pojo copy of object

        idMap[id.toString()] = pojo;

        // Enumerate all template properties for the object
        var template = obj.__template__;
        var templateName = template.__name__;
        var props = template.getProperties();
        var ix, foreignKey;
        for (var prop in props) {
            var defineProperty = props[prop];
            var isCrossDocRef = Schema.isCrossDocRef(persistor, template, prop, defineProperty);
            var value = obj[prop];

            if (!UtilityFunctions._persistProperty(persistor, defineProperty) || !defineProperty.enumerable || typeof (value) == 'undefined' || value == null) {

                // Make sure we don't wipe out foreign keys of non-cascaded object references
                if (defineProperty.type != Array &&
                    defineProperty.type && defineProperty.type.isObjectTemplate &&
                    !(!isCrossDocRef || !defineProperty.type.__schema__.documentOf) &&
                    obj[prop + 'Persistor'] && !obj[prop + 'Persistor'].isFetched && obj[prop + 'Persistor'].id &&
                    !(!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id)) {
                    pojo[schema.parents[prop].id] = new ObjectID(obj[prop + 'Persistor'].id.toString())
                    continue;
                }
                if (!UtilityFunctions._persistProperty(persistor, defineProperty) || !defineProperty.enumerable || typeof (value) == 'undefined')
                    continue;
            }

            // For arrays we either just copy each element or link and save each element
            if (defineProperty.type == Array) {
                if (!defineProperty.of)
                    throw new Error(templateName + '.' + prop + " is an Array with no 'of' declaration");

                // If type of pojo
                if (!defineProperty.of.__collection__)
                    pojo[prop] = value;

                // Is this a subdocument
                else if (!isCrossDocRef || !defineProperty.of.__schema__.documentOf) {
                    pojo[prop] = [];
                    if (value) {
                        var values = pojo[prop];
                        for (ix = 0; ix < value.length; ++ix) {
                            // Is this a sub-document being treated as a cross-document reference?
                            // If so it's foreign key gets updated with our id
                            if (isCrossDocRef) {

                                (usedLogger).debug({
                                    component: 'persistor', module: 'update.persistSaveMongo',
                                    activity: 'processing'
                                }, 'Treating ' + prop + ' as cross-document sub-document');

                                // Get the foreign key to be updated
                                if (!schema || !schema.children || !schema.children[prop] || !schema.children[prop].id)
                                    throw new Error(templateName + '.' + prop + ' is missing a children schema entry');
                                foreignKey = schema.children[prop].id;

                                // If not up-to-date put in our id
                                if (!value[ix][foreignKey] || value[ix][foreignKey].toString() != id.toString()) {
                                    value[ix][foreignKey] = id;
                                    value[ix].__dirty__ = true;
                                    (usedLogger).debug({
                                        component: 'persistor', module: 'update.persistSaveMongo',
                                        activity: 'processing'
                                    }, 'updated it\'s foreign key');
                                }

                                // If we were waiting to resolve where this should go let's just put it here
                                if ((typeof (value[ix]._id) == 'function')) {   // This will resolve the id and it won't be a function anymore
                                    (usedLogger).debug({
                                        component: 'persistor', module: 'update.persistSaveMongo',
                                        activity: 'processing'
                                    }, prop + ' waiting for placement, ebmed as subdocument');
                                    values.push(persistSave(persistor, value[ix], promises, masterId, idMap, txn, logger));
                                }
                                // If it was this placed another document or another place in our document
                                // we don't add it as a sub-document
                                if (value[ix]._id && (idMap[value[ix]._id.toString()] ||    // Already processed
                                    value[ix]._id.replace(/:.*/, '') != masterId))          // or in another doc
                                {
                                    if (value[ix].__dirty__) // If dirty save it
                                        promises.push(persistSave(persistor, value[ix], promises, null, idMap, txn, logger));
                                    continue;  // Skip saving it as a sub-doc
                                }
                                // Save as sub-document
                                (usedLogger).debug({
                                    component: 'persistor', module: 'update.persistSaveMongo',
                                    activity: 'processing'
                                }, 'Saving subdocument ' + prop);
                                values.push(persistSave(persistor, value[ix], promises, masterId, idMap, txn, logger));
                            } else {
                                if (value[ix]._id && idMap[value[ix]._id.toString()]) // Previously referenced objects just get the id
                                    values.push(value[ix]._id.toString());
                                else // Otherwise recursively obtain pojo
                                    values.push(persistSave(persistor, value[ix], promises, masterId, idMap, txn, logger));
                            }

                        }
                    }
                    // Otherwise this is a database reference and we must make sure that the
                    // foreign key points back to the id of this entity
                } else {
                    if (value instanceof Array)
                        for (ix = 0; ix < value.length; ++ix) {
                            if (!schema || !schema.children || !schema.children[prop] || !schema.children[prop].id)
                                throw new Error(obj.__template__.__name__ + '.' + prop + ' is missing a children schema entry');
                            foreignKey = schema.children[prop].id;
                            if (!value[ix][foreignKey] || value[ix][foreignKey].toString() != id.toString()) {
                                value[ix][foreignKey] = persistor._id;
                                value[ix].__dirty__ = true;
                            }
                            if (value[ix].__dirty__) {
                                usedLogger.debug({
                                    component: 'persistor', module: 'update.persistSaveMongo',
                                    activity: 'processing'
                                }, 'Saving ' + prop + ' as document because we updated it\'s foreign key');
                                promises.push(persistSave(persistor, value[ix], promises, null, idMap, txn, logger));
                            }
                        }
                }
            }
            // One-to-One or Many-to-One
            else if (defineProperty.type && defineProperty.type.isObjectTemplate) {
                foreignKey = (schema.parents && schema.parents[prop]) ? schema.parents[prop].id : prop;

                if (!isCrossDocRef || !defineProperty.type.__schema__.documentOf)  // Subdocument processing:
                {

                    // If already stored in this document or stored in some other document make reference an id
                    if (value == null)
                        pojo[foreignKey] = null;
                    else if (value._id && (idMap[value._id.toString()] || value._id.replace(/:.*/, '') != masterId))
                        pojo[foreignKey] = value._id.toString();

                    // otherwise as long as in same collection just continue saving the sub-document
                    else if (defineProperty.type.__collection__ == collection)
                        pojo[foreignKey] = persistSave(persistor, value, promises, masterId, idMap, txn, logger);

                    // If an a different collection we have to get the id generated
                    else {
                        // This should cause an id to be generated eventually
                        promises.push(persistSave(persistor, value, promises, null, idMap, txn, logger));
                        // If it is not generated then queue up a function to set it when we get 'round to it
                        (function () {
                            var closureId = value._id;
                            var closurePojo = pojo;
                            var closureForeignKey = foreignKey;
                            if (!closureId || typeof (closureId == 'function'))
                                value._id = function (value) {
                                    closurePojo[closureForeignKey] = value;
                                    if (typeof (closureId) == 'function')
                                        closureId.call(null, value);
                                }
                            else
                                pojo[foreignKey] = value._id.toString();
                        })();
                    }

                } else {   // Otherwise this is a database reference and we must make sure that we
                    // have a foreign key that points to the entity
                    if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id)
                        throw new Error(obj.__template__.__name__ + '.' + prop + ' is missing a parents schema entry');

                    foreignKey = schema.parents[prop].id;
                    // Make sure referenced entity has an id
                    if (value && !value._id) {
                        value._id = UtilityFunctions.getDBID().toString(); // Create one
                        value.__dirty__ = true;     // Will need to be saved
                    }
                    // Make sure we point to that id
                    if (!obj[foreignKey] || obj[foreignKey].toString != value._id.toString()) {
                        obj[foreignKey] = value ? value._id.toString() : null;
                    }
                    pojo[foreignKey] = value ? new ObjectID(obj[foreignKey]) : null;
                    if (value && value.__dirty__)
                        promises.push(persistSave(persistor, value, promises, null, idMap, txn, logger));
                }
            }
            else if (defineProperty.type == Date)
                pojo[prop] = obj[prop] ? obj[prop].getTime() : null;
            else
                pojo[prop] = obj[prop];
        }

        if (savePOJO)
            promises.push(save(persistor, obj, pojo, isDocumentUpdate ? new ObjectID(obj._id) : null, txn, logger));
        if (resolvePromises)
            return UtilityFunctions.resolveRecursivePromises(promises, pojo).then(function (pojo) {
                pojo._id = obj._id;
                return pojo;
            });
        else
            return pojo;
    }

    // PersistObjectTemplate.getFromPersistWithMongoId 
    export async function findById (persistor: typeof PersistObjectTemplate, template, id, cascade, isTransient, idMap, _logger) {
        const objectId = new ObjectID(id.toString());
        const results = await findByQuery(persistor, template, {_id: objectId}, cascade, null, null, isTransient, idMap)

        return results[0];
    };

    // getFromPersistWithQuery
    export async function findByQuery(persistor: typeof PersistObjectTemplate, template, query, cascade, skip, limit, isTransient, idMap, options?, logger?) {
        idMap = idMap || {};
        options = options || {};
        if (typeof(skip) != 'undefined')
            options.skip = skip * 1;
        if (typeof(limit) != 'undefined')
            options.limit = limit * 1;
        if (template.__schema__.subDocumentOf) {
            var subQuery = createSubDocQuery(query, template, logger);
            return this.getPOJOFromMongoQuery(template, subQuery.query, options, logger).then(function(pojos) {
                var promises = [];
                var results = [];
                for (var ix = 0; ix < pojos.length; ++ix) {

                    // Populate the idMap for any references
                    if (!idMap[pojos[ix]._id.toString()]) {
                        var topType = this.getTemplateByCollection(template.__collection__);
                        this.getTemplateFromMongoPOJO(pojos[ix], topType, promises, {type: topType}, idMap, {},
                            null, null, isTransient)
                    }
                    var subPojos = this.getPOJOSFromPaths(template, subQuery.paths, pojos[ix], query);
                    for (var jx = 0; jx < subPojos.length; ++jx) {
                        promises.push(this.getTemplateFromMongoPOJO(subPojos[jx], template, null, null, idMap, cascade, null, null, isTransient, logger).then(function (pojo) {
                            results.push(pojo);
                        }));
                    }
                }
                return this.resolveRecursivePromises(promises, results);
            }.bind(this));
        } else
            return this.getPOJOFromMongoQuery(template, query, options, logger).then(function(pojos)
            {
                var promises = [];
                var results = [];
                for (var ix = 0; ix < pojos.length; ++ix)
                    (function () {
                        var cix = ix;
                        promises.push(this.getTemplateFromMongoPOJO(pojos[ix], template, null, null, idMap, cascade, null, null, isTransient, logger).then(function (obj) {
                            results[cix] = obj;
                        }))
                    }.bind(this))();
                return this.resolveRecursivePromises(promises, results);

            }.bind(this));
    }

    function isObjectID (elem) {
        return elem &&  (elem instanceof ObjectID || elem._bsontype)
    }

    function traverse(templates, targetTemplate, paths, template, queryString) {
        var props = template.getProperties();
        for (var prop in props) {
            var defineProperty = props[prop];
            var propTemplate = defineProperty.of || defineProperty.type;
            if (propTemplate && propTemplate.__name__ &&
                !templates[template.__name__ + '.' + prop] && propTemplate.__schema__ && propTemplate.__schema__.subDocumentOf) {
                if (propTemplate == targetTemplate)
                    paths.push(queryString + prop);
                templates[template.__name__ + '.' + prop] = true;
                traverse(templates, targetTemplate, paths, propTemplate, queryString + prop + '.')
            }
        }
    }
        

    function queryTraverse(path, newQuery, query) {
        for (var prop in query) {
            var newProp = path + '.' + prop;
            var elem = query[prop];

            if (prop.match(/\$(gt|lt|gte|lte|ne|in)/i)) {
                newQuery[prop] = elem;
            }
            else if (typeof (elem) == 'string' || typeof(elem) == 'number' || isObjectID(elem)) {
                newQuery[newProp] = elem;
            }
            else if (elem instanceof Array) { // Should be for $and and $or
                newQuery[prop] = [];
                for (var ix = 0; ix < elem.length; ++ix) {
                    newQuery[prop][ix] = {}
                    queryTraverse(path, newQuery[prop][ix], elem[ix]);
                }
            } else { // this would be for sub-doc exact matches which is unlikely but possible
                newQuery[newProp] = {};
                queryTraverse(path, newQuery[newProp], elem)
            }
        }
    }

    /**
     * Create a query for a sub document by find the top level document and traversing to all references
     * building up the query object in the process
     * @param {object} targetQuery - the query where this not a sub-document (e.g. key: value)
     * @param {object} targetTemplate - the template to which it applies
     * @param {object} logger object template logger
     * @returns {*}
     */
    export function createSubDocQuery (targetQuery, targetTemplate, logger) {
        var topTemplate = targetTemplate.__topTemplate__;

        // Build up an array of string paths that traverses down to the desired template
        const paths = [];
        var templates = {};
        traverse(templates, targetTemplate, paths, topTemplate, '');
        // Walk through the expression substituting the path for any refs
        var results = {paths: [], query: {'$or' : []}};
        for (var ix = 0; ix < paths.length; ++ix)
        {
            var path = paths[ix];
            results.paths.push(path);
            var newQuery = {};

            paths[ix] = {};
            if (targetQuery) {
                queryTraverse(newQuery, targetQuery);
                results.query['$or'].push(newQuery);
                (logger || this.logger).debug({component: 'persistor', module: 'query', activity: 'processing'}, 'subdocument query for ' + targetTemplate.__name__ + '; ' + JSON.stringify(results.query));
            }
        }
        return results;
    }


      /**
     * Extract query and options out of cascade spec and return new subordinate cascade spec
     *
     * @param {object} query to fill in
     * @param {object} options to fill in
     * @param {object} parameterFetch options specified in call
     * @param {object} schemaFetch options specified in schema
     * @param {object} propertyFetch options specified in template
     * @returns {{}}
     */

    // processCascade
    export async function processCascade(query, options, parameterFetch, schemaFetch, propertyFetch) {

        var fetch: any = {}; // Merge fetch specifications in order of priority
        var prop;

        if (propertyFetch) {
            for (prop in propertyFetch) {
                fetch[prop] = propertyFetch[prop];
            }
        }

        if (schemaFetch) {
            for (prop in schemaFetch) {
                fetch[prop] = schemaFetch[prop];
            }
        }

        if (parameterFetch) {
            for (prop in parameterFetch) {
                fetch[prop] = parameterFetch[prop];
            }
        }

        var newCascade = {}; // Split out options, query and cascading fetch

        for (var option in fetch)
            switch (option)
            {
            case 'fetch':
                newCascade = fetch.fetch;
                break;

            case 'query':
                for (prop in fetch.query) {
                    query[prop] = fetch.query[prop];
                }
                break;

            default:
                options[option] = fetch[option];

            }
        return newCascade;
    }

    /**
     * Remove objects from a collection/table
     *
     * @param {SuperType} template object to delete
     * @param {json} query mongo style queries
     * @param {object} logger objecttemplate logger
     * @returns {object} commandresult of mongo client
     */

    // deleteFromPersistWithMongoQuery
    export async function deleteByQuery(persistor: typeof PersistObjectTemplate, template, query, logger) {
        const objs = await findByQuery(template, query, undefined, undefined, undefined, undefined, undefined, undefined, logger);

        const deleted = objs.map(async (obj) => await obj.persistDelete());

        return await Promise.all(deleted);
    };

    /**
     * Remove object from a collection/table
     *
     * @param {SuperType} template object to delete
     * @param {string} id mongo id
     * @param {object} logger objecttemplate logger
     * @returns {object} commandresult of mongo client
     */

     // deleteFromPersistWithMongoId
    export async function deleteById (persistor: typeof PersistObjectTemplate, template, id, logger) {
        return deleteByQuery(persistor, template, { _id: new ObjectID(id.toString()) }, logger);
    }
}