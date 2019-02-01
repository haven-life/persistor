import { UtilityFunctions } from './UtilityFunctions';

export namespace Mongo {
    /* Mongo implementation of save */
    export async function savePojoToMongo(persistor, obj, pojo, updateID, _txn, logger) {
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
}