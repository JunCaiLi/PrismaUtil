const {Prisma} = require('@prisma/client');

class SqlUtil {
  /**
   * @param {num} pageNumber
   * @param {num} pageSize
   * @param {object} conditions
   * @param {Prisma.CompaniesDelegate<ExtArgs>} collection
   * @param {object} orderByField
   * @param {[]} rangeField
   * @param {string []} ListField
   * @param {string []} addressField
   * @param {string []} logicAndListField
   * @param {string []} searchField
   * @param {function} onDataProcess
   * @returns
   */
  async fetchDataWithPagination(
    pageNumber,
    pageSize,
    conditions,
    collection,
    orderByField,
    rangeField,
    ListField,
    addressField,
    logicAndListField,
    searchField,
    onDataProcess,
  ) {
    const {take, skip} = this.getPagination(pageNumber, pageSize);

    const query = await this.handleConditions(
      conditions,
      rangeField,
      ListField,
      addressField,
      logicAndListField,
      searchField,
    );
    const data = await collection.findMany({
      where: query,
      orderBy: orderByField,
      take,
      skip,
    });

    const totalCount = await collection.count({where: query});
    const totalPages = Math.ceil(totalCount / pageSize);

    const result = onDataProcess?.(data) ?? data;

    return {
      data: result,
      total: totalCount,
      totalPages,
      currentPage: pageNumber,
    };
  }

  /**
   * create data
   * @param {Prisma} prisma
   * @param {Prisma.CompaniesDelegate<ExtArgs>} collection
   * @param {object} data
   * @returns
   */
  async createData(prisma, collection, data) {
    try {
      const newData = await collection.create({
        data: data,
      });
      return {success: true, data: newData};
    } catch (error) {
      console.error('Error creating data:', error);
      return {success: false, error: 'Error creating data'};
    } finally {
      await prisma.$disconnect();
    }
  }

  async batchCreateData(prisma, collection, listData) {
    try {
      // Batch create data
      await collection.createMany({
        data: listData,
      });
      return {success: true};
    } catch (error) {
      console.error('Error creating users:', error);
      return {success: false, error: 'Error creating data'};
    } finally {
      // Close the Prisma client connection
      await prisma.$disconnect();
    }
  }


  /**
   * Physical deletion
   * @param {Prisma} prisma
   * @param {*} collection
   * @param {*} id
   * @returns
   */
  async deleteData(prisma, collection, id) {
    try {
      await collection.delete({
        where: {id},
      });

      return {success: true};
    } catch (error) {
      console.error('Error deleting data:', error);
      return {success: false, error: 'Error deleting data'};
    } finally {
      await prisma.$disconnect();
    }
  }

  /**
   * update collection
   * @param {Prisma} prisma
   * @param {Prisma.CompaniesDelegate<ExtArgs>} collection
   * @param {object} dataToUpdate
   * @param {string} id
   * @returns {object}
   */
  async updateFields(prisma, collection, dataToUpdate, id) {
    try {
      // Use the Prisma updateMany method to update the fields
      await collection.updateMany({
        where: {
          id: id,
        },
        data: dataToUpdate,
      });
      return {success: true};
    } catch (error) {
      console.error(error);
      return {success: false};
    } finally {
      // disconnect the Prisma client
      await prisma.$disconnect();
    }
  }

  /**
   * @param {number} pageNumber
   * @param {number} pageSize
   * @returns
   */
  getPagination(pageNumber, pageSize) {
    const page = pageNumber || 1;
    const limit = pageSize || 20;
    const skip = (page - 1) * limit;
    const take = limit;
    return {take, skip};
  }

  /**
   *
   * @param {{}} conditions
   * @param {[]} rangeField
   * @param {string []} ListField
   * @param {string []} addressField
   * @param {string []} logicAndListField
   * @param {string []} searchField
   * @returns
   */
  async handleConditions(
    conditions,
    rangeField,
    ListField,
    addressField,
    logicAndListField,
    searchField,
  ) {
    const where = {};
    for (const key in conditions) {
      if (Object.prototype.hasOwnProperty.call(conditions, key)) {
        const value = conditions[key];
        if (addressField.includes(key)) {
          // Prisma sentence OR/AND, OR as ||, AND as &&
          where['OR'] = value.map((param) => {
            let result = this.removeNullValues(param);
            if (param.country && !param.province && !param.city) {
              return {
                [key]: {
                  path: ['country'],
                  equals: param.country
                }
              }
            }
            return {
              [key]: {
                equals: result, // match all JSON object
              },
            }
          });
        } else if (Array.isArray(value)) {
          // if rangeField is DateTime, value's type is string and must be ISO8601String  e.g.: 1969-07-20T20:18:04.000Z
          if (rangeField.includes(key)) {
            where[key] = {
              gte: value[0],
              lte: value[1],
            };
          }
          // when value is array, it means logic AND
          else if (logicAndListField.includes(key)) {
            where[key] = {hasEvery: value};
          } else if (ListField.includes(key)) {
            where[key] = {hasSome: value};
          } else {
            // for database field not array type, but use array search
            where[key] = {in: value};
          }
        } else {
          if (searchField.includes(key)) {
            // partial match
            where[key] = {contains: value}
          } else {
            // full equals
            where[key] = {equals: value};
          }
        }
      }
    }
    return where;
  }

  /**
   * 校验查询逻辑，仅支持prisma支持逻辑
   * @param {*} data
   * @param {{
   *   allFields?: string[],
   * }}options
   * @return {{[p: string]: *}}
   */
  checkConditions(data, options) {
    const allFields = [...(options.allFields ?? []), 'AND', 'OR', 'NOT'];

    const result = Reflect.ownKeys(data).map((k) => `${k}`);

    return result.every((k) => allFields.includes(k));
  }

  /**
   * data transform
   * @param {*} data
   * @param {{
   *   dateFields?: string[];
   *   allFields?: string[],
   * }}options
   * @return {{[p: string]: *}}
   */
  transformData(data, options) {
    const {dateFields = [], allFields = []} = options ?? {};

    const result = {...data};

    for (const key in result) {
      // remove field when key not contain in the map
      if (!allFields.includes(key)) {
        delete result[key];
      }
      // remove empty field
      const val = result[key];
      if (val === null || val === undefined) {
        delete result[key];
      }
      // handle Date Time
      if (dateFields.includes(key)) {
        result[key] = this.toSQLDateTime(result[key]);
      }
    }
    return result;
  }

  /**
   * @param {string || {}} input
   * @returns
   */
  toSQLDateTime(input) {
    if (typeof input === "string") {
      const timestamp = Date.parse(input);
      if (!isNaN(timestamp)) {
        return new Date(timestamp);
      } else {
        return null;
      }
    } else if (typeof input === "object" && input !== null && "_seconds" in input) {
      const seconds = input['_seconds'];
      return new Date(seconds * 1000);
    } else {
      return null
    }
  }

  /**
   * handle insert into fields
   * @param {number} length
   * @returns {string} VALUES($1, $2, $3)
   */
  generateValuesClause(length) {
    if (length <= 0) {
      return '';
    }

    const values = Array.from({length}, (_, index) => `$${index + 1}`).join(', ');
    return `VALUES(${values})`;
  }

  /**
   * removeNullValues
   * @param {*} obj
   * @returns
   */
  removeNullValues(obj) {
    const newObj = {};

    Object.keys(obj).forEach(key => {
      if (obj[key] !== null) {
        newObj[key] = obj[key];
      }
    });

    return newObj;
  }
}


module.exports = new SqlUtil();
