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
    export async function countByQuery(persistor: typeof PersistObjectTemplate, template, query, logger?) {
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

        const pojos = await persistor.getPOJOFromQuery(template, query, idMap);

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
     * 
     * persistSaveMongo
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
                `Duplicate processing of ${obj.__template__.__name__}:${id.toString()}`);
            return idMap[id.toString()];
        }

        usedLogger.debug(
            {
                component: 'persistor',
                module: 'update.persistSaveMongo',
                activity: 'save'
            },
            `Saving ${obj.__template__.__name__}:${id.toString()} master_id=${masterId}`);

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

            if (!UtilityFunctions._persistProperty(defineProperty) || !defineProperty.enumerable || typeof (value) == 'undefined' || value == null) {

                // Make sure we don't wipe out foreign keys of non-cascaded object references
                if (defineProperty.type != Array && defineProperty.type && defineProperty.type.isObjectTemplate &&
                    !(!isCrossDocRef || !defineProperty.type.__schema__.documentOf) &&
                    obj[prop + 'Persistor'] && !obj[prop + 'Persistor'].isFetched && obj[prop + 'Persistor'].id &&
                    !(!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id)) {
                    pojo[schema.parents[prop].id] = new ObjectID(obj[prop + 'Persistor'].id.toString());
                    continue;
                }
                if (!UtilityFunctions._persistProperty(defineProperty) || !defineProperty.enumerable || typeof (value) == 'undefined')
                    continue;
            }

            // For arrays we either just copy each element or link and save each element
            if (defineProperty.type == Array) {
                if (!defineProperty.of)
                    throw new Error(`${templateName}.${prop} is an Array with no 'of' declaration`);

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

                                usedLogger.debug({
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
                                    usedLogger.debug({
                                        component: 'persistor', module: 'update.persistSaveMongo',
                                        activity: 'processing'
                                    }, 'updated it\'s foreign key');
                                }


                                // If we were waiting to resolve where this should go let's just put it here
                                if ((typeof (value[ix]._id) == 'function')) {   // This will resolve the id and it won't be a function anymore
                                    usedLogger.debug({
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
                                usedLogger.debug({
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
                        pojo[foreignKey] = await persistSave(persistor, value, promises, masterId, idMap, txn, logger);

                    // If an a different collection we have to get the id generated
                    else {
                        // This should cause an id to be generated eventually
                        promises.push(persistSave(persistor, value, promises, null, idMap, txn, logger));
                        // If it is not generated then queue up a function to set it when we get 'round to it

                        {
                            const closureId = value._id;
                            const closurePojo = pojo;
                            const closureForeignKey = foreignKey;
                            if (!closureId || typeof (closureId == 'function')) {
                                value._id = (value) => {
                                    closurePojo[closureForeignKey] = value;
                                    if (typeof (closureId) == 'function') {
                                        closureId(value);
                                    }
                                }
                            }
                            else {
                                pojo[foreignKey] = value._id.toString();
                            }
                        }
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


        // @TODO: we may not resolvePromises, is that alright?
        if (resolvePromises) {
            const resolvedPojo = await UtilityFunctions.resolveRecursivePromises(promises, pojo);
            resolvedPojo._id = obj._id;
            return resolvedPojo;
        }
        else
            return await pojo;
    }

    // Query.ts

    // PersistObjectTemplate.getFromPersistWithMongoId 
    export async function findById(persistor: typeof PersistObjectTemplate, template, id, cascade, isTransient, idMap, _logger) {
        const objectId = new ObjectID(id.toString());
        const results = await findByQuery(persistor, template, { _id: objectId }, cascade, null, null, isTransient, idMap)

        return results[0];
    };

    // getFromPersistWithMongoQuery @TODO: come back to this later
    export async function findByQuery(persistor: typeof PersistObjectTemplate, template, query, cascade, skip, limit, isTransient, idMap, options?, logger?) {
        idMap = idMap || {};
        options = options || {};
        if (typeof (skip) != 'undefined')
            options.skip = skip * 1;
        if (typeof (limit) != 'undefined')
            options.limit = limit * 1;
        if (template.__schema__.subDocumentOf) {
            var subQuery = createSubDocQuery(persistor, query, template, logger);
            const pojos: any[] = await getPOJOByQuery(persistor, template, subQuery.query, options, logger);

            // var promises = [];
            var results = [];
            let totalPromises: Promise<any>[];
            let index = 0;
            const promises = pojos.map(async (pojo, ix) => {

                // Populate the idMap for any references
                if (!idMap[pojo._id.toString()]) {
                    var topType = UtilityFunctions.getTemplateByCollection(persistor, template.__collection__);
                    await getTemplateFromPOJO(persistor, pojo, topType, promises, { type: topType }, idMap, {}, null, null, isTransient)
                }
                var subPojos = getPOJOSFromPaths(persistor, template, subQuery.paths, pojo, query);

                const subPromises = subPojos.map(async (subPojo, jx) => {
                    const gotTemplate = await getTemplateFromPOJO(persistor, subPojo, template, null, null, idMap, cascade, null, null, isTransient, logger);
                    results.push(gotTemplate);
                });

                return await Promise.all(subPromises);
            });

            return UtilityFunctions.resolveRecursivePromises(promises, results);
        } else {
            const pojos: any[] = await getPOJOByQuery(persistor, template, query, options, logger);
            var results = [];

            // can replace this with map;
            const promises = pojos.map(async (pojo, index) => {
                const obj = await getTemplateFromPOJO(persistor, pojo, template, null, null, idMap, cascade, null, null, isTransient, logger);
                results[index] = obj;
            });

            return UtilityFunctions.resolveRecursivePromises(promises, results);
        }
    }

    function copyProps(obj) {
        var newObj = {};
        for (var prop in obj)
            newObj[prop] = obj[prop];
        return newObj;
    }

    /**
     * closureProp = prop,
     * closureOf = defineProperty.of
     * closureDefineProperty = defineProperty
     * closruePersistorProp = persistorPropertyName
     */
    async function helper(persistor: typeof PersistObjectTemplate, closureProp, closurePersistorProp, cascadeFetch, schema, defineProperty, obj, promises, query, options, idMap, isTransient, logger) {

        let closureDefineProperty = defineProperty;
        var closureOf = closureDefineProperty.of;
        var closureCascade = UtilityFunctions.processCascade(query, options, cascadeFetch, (schema && schema.children) ? schema.children[closureProp].fetch : null, defineProperty.fetch);
        var closureIsSubDoc = !!closureDefineProperty.of.__schema__.subDocumentOf;
        obj[closureProp] = [];

        // For subdocs we have to build up a query to fetch all docs with these little buggers
        if (closureIsSubDoc) {
            var closureOrigQuery = query;
            var results = createSubDocQuery(persistor, query, closureDefineProperty.of, logger);
            query = results.query;
            var closurePaths = results.paths;
        }
        const pojos = await getPOJOByQuery(persistor, defineProperty.of, query, options, logger);

        // For subdocs we have to fish them out of the documents making sure the query matches
        if (closureIsSubDoc) {
            obj[closureProp] = [];
            for (var ix = 0; ix < pojos.length; ++ix) {
                // Populate the idMap for any references
                if (!idMap[pojos[ix]._id.toString()]) {
                    var topType = UtilityFunctions.getTemplateByCollection(persistor, closureOf.__collection__);
                    await getTemplateFromPOJO(persistor, pojos[ix], topType, promises, { type: topType }, idMap, {},
                        null, null, isTransient, logger)
                }
                // Grab the actual Pojos since may not be avail from processing parent
                var subPojos = getPOJOSFromPaths(persistor, defineProperty.of, closurePaths, pojos[ix], closureOrigQuery)
                for (var jx = 0; jx < subPojos.length; ++jx)
                    // Take them from cache or fetch them
                    obj[closureProp].push((!closureCascade && idMap[subPojos[jx]._id.toString()]) ||
                        await getTemplateFromPOJO(persistor, subPojos[jx], closureDefineProperty.of,
                            promises, closureDefineProperty, idMap, closureCascade, null, null, isTransient, logger));
            }
        }
        else {
            for (ix = 0; ix < pojos.length; ++ix) {
                // Return cached one over freshly read
                obj[closureProp][ix] = idMap[pojos[ix]._id.toString()] ||
                    await getTemplateFromPOJO(persistor, pojos[ix], closureDefineProperty.of,
                        promises, closureDefineProperty, idMap, closureCascade, null, null, isTransient, logger)
            }
        }

        obj[closurePersistorProp].isFetched = true;
        obj[closurePersistorProp].start = options ? options.start || 0 : 0;
        obj[closurePersistorProp].next = obj[closurePersistorProp].start + pojos.length;
        obj[closurePersistorProp] = copyProps(obj[closurePersistorProp]);

        // not sure if this is right syntax
        return promises.push(true);

    }

    /**
     * Enriches a "Plane Old JavaScript Object (POJO)" by creating it using the new Operator
     * so that all prototype information such as functions are created. It will reconstruct
     * references one-to-one and one-two-many by reading them from the database
     *  *
     * @param {object} pojo is the unadorned object
     * @param {object} template is the template used to create the object
     * @param {object} promises Array of pending requests
     * @param {object} defineProperty {@TODO need to check}
     * @param {object} idMap object mapper for cache
     * @param {object} cascade fetch spec.
     * @param {object} establishedObj {@TODO need to review, used for amorphic}
     * @param {unknown} specificProperties {@TODO need to review}
     * @param {bool} isTransient unknown.
     * @param {object} logger object template logger
     * @returns {*} an object via a promise as though it was created with new template()
     * 
     * getTemplateFromMongoPOJO
     */

    export async function getTemplateFromPOJO(persistor: typeof PersistObjectTemplate, pojo, template, promises, defineProperty, idMap, cascade, establishedObj, specificProperties, isTransient, logger?) {

        const usedLogger = logger || persistor.logger;
        // For reco
        // rding back refs
        if (!idMap) {
            throw new Error('Missing idMap on getTemplateFromPOJO (mongo)');
        }
        var topLevel = false;
        if (!promises) {
            topLevel = true;
            promises = [];
        }

        // Create the new object with correct constructor using embedded ID if ObjectTemplate
        const templateId = `persist${template.__name__}-${pojo._template.replace(/.*:/, '')}-${pojo._id.toString()}`;
        var obj = establishedObj || idMap[pojo._id.toString()] || persistor._createEmptyObject(template, templateId, defineProperty, isTransient);

        // Once we find an object already fetch that is not transient query as normal for the rest
        if (!obj.__transient__ && !establishedObj && !isTransient)
            isTransient = false;

        var collection = obj.__template__.__collection__;
        var schema = obj.__template__.__schema__;

        var id = null;
        if (pojo._id) { // If object is persistent make sure id is a string and in map
            id = pojo._id;
            obj._id = id.toString();

            // If we have a real value and an array of value store functions, call them
            if (idMap[id.toString()] && idMap[id.toString()] instanceof Array)
                for (var fx = 0; fx < idMap[id.toString()].length; ++fx)
                    idMap[id.toString()][fx].call(null, obj);

            idMap[id.toString()] = obj;
        }
        if (pojo.__version__)
            obj.__version__ = pojo.__version__;

        // Go through all the properties and transfer them to newly created object
        var props = specificProperties || obj.__template__.getProperties();
        var ix, options, cascadeFetchProp, query;
        for (var prop in props) {
            //if (prop.match(/Persistor$/))
            //    continue;

            var value = pojo[prop];
            defineProperty = props[prop];
            var type = defineProperty.type;
            var isCrossDocRef = Schema.isCrossDocRef(persistor, obj.__template__, prop, defineProperty) || defineProperty.autoFetch;
            var cascadeFetch = (cascade && cascade[prop]) ? cascade[prop] : null;
            var doFetch = defineProperty['fetch'] || cascadeFetch;

            var persistorPropertyName = prop + 'Persistor';
            obj[persistorPropertyName] = obj[persistorPropertyName] || { count: 0 };

            // Make sure this is property is persistent and that it has a value.  We have to skip
            // undefined values in case a new property is added so it can retain it's default value
            if (!UtilityFunctions._persistProperty(defineProperty) || !defineProperty.enumerable ||
                (!isCrossDocRef && (typeof (value) == 'undefined')))
                continue;
            if (!type)
                throw new Error(obj.__template__.__name__ + '.' + prop + ' has no type decleration');

            if (type == Array) {
                // If type of pojo
                if (!defineProperty.of.__collection__)
                    obj[prop] = value;
                // If this is in the same entity just copy over
                else if (!isCrossDocRef) {
                    obj[prop] = [];
                    for (ix = 0; ix < pojo[prop].length; ++ix) {
                        // Did we get a value ?
                        if (pojo[prop][ix]) {

                            // is it a cached id reference
                            if (typeof (pojo[prop][ix]) == 'string') {
                                // If nothing in the map create an array
                                if (!idMap[pojo[prop][ix]])
                                    idMap[pojo[prop][ix]] = [];

                                // If an array of value store functions add ours to the list
                                if (idMap[pojo[prop][ix]] instanceof Array)
                                    (function () {
                                        var closureIx = ix;
                                        var closureProp = prop;
                                        idMap[pojo[prop][ix]].push(function (value) {
                                            pojo[closureProp][closureIx] = value;
                                        });
                                    })()
                                else
                                    obj[prop][ix] = idMap[pojo[prop][ix]];
                            } else {
                                options = defineProperty.queryOptions || {};
                                cascadeFetchProp = UtilityFunctions.processCascade(query, options, cascadeFetch, null, defineProperty.fetch);
                                obj[prop][ix] = idMap[pojo[prop][ix]._id.toString()] ||
                                    getTemplateFromPOJO(persistor, pojo[prop][ix], defineProperty.of, promises, defineProperty, idMap,
                                        cascadeFetchProp, null, null, isTransient, logger);
                            }
                        } else
                            obj[prop][ix] = null;
                    }
                }
                // Otherwise this is a database reference and we have to find the collection of kids
                else {
                    var self = this;

                    {
                        const closurePersistorProp = persistorPropertyName;
                        const closureOf = defineProperty.of;
                        const closureDefineProperty = defineProperty;
                        const closureIsSubDoc = !!closureDefineProperty.of.__schema__.subDocumentOf;
                        if (closureIsSubDoc) {
                            obj[closurePersistorProp] = copyProps(obj[closurePersistorProp]);
                        }
                        else {
                            // @TODO: might be buggy;
                            const count = await countByQuery(persistor, closureOf, query);
                            obj[closurePersistorProp].count = count;
                            obj[closurePersistorProp] = copyProps(obj[closurePersistorProp]);

                            promises.push(Promise.resolve());
                        }
                    }

                    if (doFetch) {
                        query = {};
                        options = {};
                        if (id) {
                            if (!schema || !schema.children || !schema.children[prop])
                                throw new Error(obj.__template__.__name__ + '.' + prop + ' is missing a children schema entry');
                            var foreignKey = schema.children[prop].id;
                            query[foreignKey] = id.toString().match(/:/) ? id.toString() : new ObjectID(id.toString());
                        }
                        usedLogger.debug({ component: 'persistor', module: 'query.getTemplateFromPOJO', activity: 'pre' },
                            'fetching ' + prop + ' cascading ' + JSON.stringify(cascadeFetch) + ' ' + JSON.stringify(query) + ' ' + JSON.stringify(options));
                        await helper(persistor, prop, persistorPropertyName, cascadeFetch, schema, defineProperty, obj, promises, query, options, idMap, isTransient, logger);
                    } else {
                        obj[persistorPropertyName].isFetched = false;
                    }

                    obj[persistorPropertyName] = copyProps(obj[persistorPropertyName]);
                }

            } else if (type.isObjectTemplate) {
                // Same collection suck in from idMap if previously referenced or process pojo
                if (type.__collection__ == collection && !isCrossDocRef) {
                    // Did we get a value ?
                    if (pojo[prop]) {

                        // is it a cached id reference
                        if (typeof (pojo[prop]) == 'string') {
                            // If nothing in the map create an array
                            if (!idMap[pojo[prop]])
                                idMap[pojo[prop]] = [];

                            // If an array of value store functions add ours to the list
                            if (idMap[pojo[prop]] instanceof Array)
                                idMap[pojo[prop]].push(function (value) {
                                    pojo[prop] = value;
                                });
                            else
                                obj[prop] = idMap[pojo[prop]];
                        } else {
                            options = defineProperty.queryOptions || {};
                            cascadeFetchProp = UtilityFunctions.processCascade(query, options, cascadeFetch, null, defineProperty.fetch);

                            obj[prop] = idMap[pojo[prop]._id.toString()] || await getTemplateFromPOJO(persistor, pojo[prop], type, promises,
                                defineProperty, idMap, cascadeFetchProp, null, null, isTransient, logger);
                        }

                    }
                    else {
                        obj[prop] = null;
                    }

                } else // Otherwise read from idMap or query for it
                {
                    // Determine the id needed
                    if (!schema || !schema.parents || !schema.parents[prop] || !schema.parents[prop].id)
                        throw new Error(obj.__template__.__name__ + '.' + prop + ' is missing a parents schema entry');

                    foreignKey = schema.parents[prop].id;

                    // ID is in pojo or else it was left in the persistor
                    var foreignId = (pojo[foreignKey] || obj[persistorPropertyName].id || '').toString();

                    // Return copy if already there
                    if (idMap[foreignId]) {
                        obj[prop] = idMap[foreignId];
                        obj[persistorPropertyName].isFetched = true;
                        obj[persistorPropertyName] = copyProps(obj[persistorPropertyName]);
                    } else {
                        if (doFetch) {  // Only fetch ahead if requested
                            obj[prop] = null;
                            if (foreignId) {
                                query = { _id: new ObjectID(foreignId.replace(/:.*/, '')) };
                                options = {};
                                usedLogger.debug({ component: 'persistor', module: 'query.getTemplateFromMongoPOJ', activity: 'processing' },
                                    'fetching ' + prop + ' cascading ' + JSON.stringify(cascadeFetch));
                                self = this;
                                {
                                    const closureProp = prop;
                                    const closureType = type;
                                    const closurePersistorProp = persistorPropertyName;

                                    const closureCascade = UtilityFunctions.processCascade(query, options, cascadeFetch, (schema && schema.parents) ? schema.parents[prop].fetch : null, defineProperty.fetch);
                                    const closureDefineProperty = defineProperty;
                                    const closureForeignId = foreignId;
                                    const closureIsSubDoc = !!closureDefineProperty.type.__schema__.subDocumentOf;

                                    // Maybe we already fetched it
                                    if (idMap[foreignId]) {
                                        obj[closureProp] = idMap[closureForeignId];
                                        obj[closurePersistorProp].isFetched = true;
                                        obj[closurePersistorProp] = copyProps(obj[closurePersistorProp]);
                                    }
                                    else {
                                        const pojos = await getPOJOByQuery(persistor, closureType, query, undefined, logger);

                                        // Assuming the reference is still there
                                        if (pojos.length > 0) {
                                            if (closureIsSubDoc) {
                                                // Process the document and the sub-document will end up in idMap
                                                if (!idMap[pojos[0]._id.toString()]) {
                                                    var topType = UtilityFunctions.getTemplateByCollection(persistor, closureType.__collection__);
                                                    await getTemplateFromPOJO(persistor, pojos[0], topType, promises, { type: topType }, idMap, {}, null, null, isTransient);
                                                }
                                                // Get actual sub-doc since it may not yet be available from processing doc
                                                const subDocQuery = createSubDocQuery(persistor, null, closureType, logger);
                                                const subDocPojo = getPOJOSFromPaths(persistor, closureType, subDocQuery.paths, pojos[0], { _id: closureForeignId } )[0];
                                                // Process actual sub-document to get cascade right and specific sub-doc
                                                if (subDocPojo && subDocPojo._id) {
                                                    if (!idMap[subDocPojo._id.toString()]) {
                                                        await getTemplateFromPOJO(persistor, subDocPojo, closureType, promises, closureDefineProperty, idMap, closureCascade, null, null, isTransient, logger);
                                                    }
                                                } else {
                                                    usedLogger.debug({ component: 'persistor', module: 'query.getTemplateFromPOJO', activity: 'processing' },
                                                        `Orphaned subdoc on ${obj.__template__.__name__}[${closureProp}:${obj._id}] foreign key: ${closureForeignId} query: ${JSON.stringify(createSubDocQuery(persistor, null, closureType, logger))}`);
                                                }
                                            } else
                                                if (!idMap[pojos[0]._id.toString()])
                                                    await getTemplateFromPOJO(persistor, pojos[0], closureType, promises, closureDefineProperty, idMap, closureCascade, null, null, isTransient, logger);
                                        }

                                        obj[closureProp] = idMap[closureForeignId];
                                        obj[closurePersistorProp].isFetched = true;
                                        obj[closurePersistorProp] = copyProps(obj[closurePersistorProp]);
                                        promises.push(Promise.resolve());
                                    }
                                }
                            } else {
                                obj[persistorPropertyName].isFetched = true;
                                obj[persistorPropertyName] = copyProps(obj[persistorPropertyName]);
                            }
                        } else {
                            obj[persistorPropertyName].isFetched = false;
                            obj[persistorPropertyName].id = foreignId;
                            obj[persistorPropertyName] = copyProps(obj[persistorPropertyName]);
                        }
                    }
                }
            } else
                if (typeof (pojo[prop]) != 'undefined') {
                    if (type == Date)
                        obj[prop] = pojo[prop] ? new Date(pojo[prop]) : null;
                    else if (type == Number)
                        obj[prop] = (!pojo[prop] && pojo[prop] !== 0) ? null : pojo[prop] * 1;
                    else if (type == Boolean)
                        obj[prop] = (pojo[prop] === true || pojo[prop] === 'true') ? true : ((pojo[prop] === false || pojo[prop] === 'false') ? false : null)
                    else
                        obj[prop] = pojo[prop];
                }
        }
        if (topLevel)
            return await UtilityFunctions.resolveRecursivePromises(promises, obj);
        else
            return obj;
    }

    function matches(pojo, query, ...args) {
        var allFound = true;
        var ix;
        for (var prop in query) {
            if (prop.toLowerCase() == '$and') {
                var allFoundAnd = true;
                for (ix = 0; ix < query[prop].length; ++ix)
                    if (matches(pojo, query[prop][ix])) { allFoundAnd = false }
                if (allFoundAnd)
                    allFound = false;
            }
            else if (prop.toLowerCase() == '$or') {
                var oneFoundOr = false;
                for (ix = 0; ix < query[prop].length; ++ix)
                    if (matches(pojo, query[prop][ix])) { oneFoundOr = true; }
                if (!oneFoundOr)
                    allFound = false;
            }
            else if (prop.toLowerCase() == '$in') {
                var isIn = false;
                for (ix = 0; ix < query[prop].length; ++ix)
                    if (query[prop][ix] == pojo)
                        isIn = true;
                if (!isIn)
                    allFound = false;
            }
            else if (prop.toLowerCase() == '$nin') {
                var notIn = true;
                for (ix = 0; ix < query[prop].length; ++ix)
                    if (query[prop][ix] == pojo)
                        notIn = false;
                if (notIn)
                    allFound = false;
            }
            else if (prop.toLowerCase() == '$gt') {
                if (!(pojo > query[prop]))
                    allFound = false;
            }
            else if (prop.toLowerCase() == '$gte') {
                if (!(pojo >= query[prop]))
                    allFound = false;
            }
            else if (prop.toLowerCase() == '$lt') {
                if (!(pojo < query[prop]))
                    allFound = false;
            }
            else if (prop.toLowerCase() == '$lte') {
                if (!(pojo <= query[prop]))
                    allFound = false;
            }
            else if (prop.toLowerCase() == '$ne') {
                if (!(pojo != query[prop]))
                    allFound = false;
            }
            else if (pojo[prop] && typeof (query[prop]) != 'string' && typeof (query[prop]) != 'number' && !(pojo[prop] instanceof ObjectID)) {
                // Sub doc all must be true
                if (!matches(pojo[prop], query[prop], false))
                    allFound = false;
            } else {
                // Otherwise the value must match
                if (!pojo[prop] || pojo[prop].toString() != query[prop].toString())
                    allFound = false;
            }
        }
        return allFound;
    }

    function traverse(pathparts, query, pojos, ref, level) {
        if (level == pathparts.length) {
            if (matches(ref, query)) {
                pojos.push(ref);
            }
        }
        else {
            ref = ref[pathparts[level]];
            if (ref instanceof Array)
                for (var jx = 0; jx < ref.length; ++jx)
                    traverse(pathparts, query, pojos, ref[jx], level + 1)
            else if (ref)
                traverse(pathparts, query, pojos, ref, level + 1);
        }
    }


    /**
     * Traverse a pojo returned form MongoDB given a set of paths and locate the sub-document
     * matching it on the original query
     * @param {object} _template is the template used to create the object
     * @param {object} paths {@TODO need to verify}
     * @param {object} pojo {@TODO need to verify}
     * @param {object} query mongo style query.
     * @returns {Array}
     * 
     * getPOJOsFromPaths
     */
    export function getPOJOSFromPaths(persistor: typeof PersistObjectTemplate, _template, paths, pojo, query) {
        var pathparts;
        var pojos = [];
        for (var ix = 0; ix < paths.length; ++ix) {
            pathparts = paths[ix].split('.');
            traverse(pathparts, query, pojos, pojo, 0);
        }
        return pojos;
    };



    function isObjectID(elem) {
        return elem && (elem instanceof ObjectID || elem._bsontype)
    }

    function traverseProps(templates, targetTemplate, paths, template, queryString) {
        var props = template.getProperties();
        for (var prop in props) {
            var defineProperty = props[prop];
            var propTemplate = defineProperty.of || defineProperty.type;
            if (propTemplate && propTemplate.__name__ &&
                !templates[template.__name__ + '.' + prop] && propTemplate.__schema__ && propTemplate.__schema__.subDocumentOf) {
                if (propTemplate == targetTemplate)
                    paths.push(queryString + prop);
                templates[template.__name__ + '.' + prop] = true;
                traverseProps(templates, targetTemplate, paths, propTemplate, queryString + prop + '.')
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
            else if (typeof (elem) == 'string' || typeof (elem) == 'number' || isObjectID(elem)) {
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
    export function createSubDocQuery(persistor: typeof PersistObjectTemplate, targetQuery, targetTemplate, logger) {
        var topTemplate = targetTemplate.__topTemplate__;

        // Build up an array of string paths that traverses down to the desired template
        const paths = [];
        var templates = {};
        traverseProps(templates, targetTemplate, paths, topTemplate, '');
        // Walk through the expression substituting the path for any refs

        var results = { paths: [], query: { '$or': [] } };
        for (var ix = 0; ix < paths.length; ++ix) {
            var path = paths[ix];
            results.paths.push(path);
            var newQuery = {};

            paths[ix] = {};
            if (targetQuery) {
                queryTraverse(path, newQuery, targetQuery);
                results.query['$or'].push(newQuery);
                const usedLogger = logger || persistor.logger;
                usedLogger.debug(
                    {
                        component: 'persistor',
                        module: 'query',
                        activity: 'processing'
                    },
                    `Subdocument query for ${targetTemplate.__name__}; ${JSON.stringify(results.query)}`);
            }
        }
        return results;
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
    export async function deleteById(persistor: typeof PersistObjectTemplate, template, id, logger) {
        return deleteByQuery(persistor, template, { _id: new ObjectID(id.toString()) }, logger);
    }
}