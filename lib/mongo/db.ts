// import { Mongo } from '../Mongo';

module.exports = function (PersistObjectTemplate) {

    // PersistObjectTemplate.savePojoToMongo = Mongo.savePojoToMongo;

    /**
     * Removes documents based on a query
     * @param {SuperType} template object to delete
     * @param {json} query mongo style queries
     * @param {object} _logger objecttemplate logger
     * @returns {object} commandresult of mongo client
     */
    PersistObjectTemplate.deleteFromMongoQuery = function(template, query, _logger) {
        var db = this.getDB(this.getDBAlias(template.__collection__)).connection;
        var collection = db.collection(this.dealias(template.__collection__));
        return collection.remove(query, {w:1, fsync:true});
    };

    PersistObjectTemplate.getPOJOFromMongoQuery = function(template, query, options, logger) {
        (logger || this.logger).debug({component: 'persistor', module: 'db', activity: 'read'}, 'db.' + template.__collection__ + '.find({" + JSON.stringify(query) + "})');
        var db = this.getDB(this.getDBAlias(template.__collection__)).connection;
        var collection = db.collection(this.dealias(template.__collection__));
        options = options || {};
        if (!options.sort)
            options.sort = {_id:1};
        return collection.find(query, null, options);
    };

    PersistObjectTemplate.countFromMongoQuery = function(template, query) {
        var db = this.getDB(this.getDBAlias(template.__collection__)).connection;
        var collection = db.collection(this.dealias(template.__collection__));
        return collection.count(query);
    };

    PersistObjectTemplate.distinctFromMongoQuery = function(template, field, query) {
        var db = this.getDB(this.getDBAlias(template.__collection__)).connection;
        var collection = db.collection(this.dealias(template.__collection__));
        return collection._collection.distinct(field, query);
    };

    PersistObjectTemplate.getPOJOFromMongoId = function (template, id, _cascade, _isTransient, idMap) {
        idMap = idMap || {};
        return this.getPOJOFromQuery(template, {_id: new this.ObjectID(id)}, idMap).then(function(pojos) {
            if (pojos.length > 0)
                return pojos[0];
            else
                return null;
        });
    }

};