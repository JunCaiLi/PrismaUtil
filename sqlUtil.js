const { Prisma, PrismaClient } = require('@prisma/client');
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
   * @param {function} onDataProcess 
   * @returns 
   */
  async fetchDataWithPagination(pageNumber, pageSize, conditions, collection, orderByField, rangeField, ListField, addressField, onDataProcess) {
    const { take, skip } = this.getPagination(pageNumber, pageSize);

    const query = await this.handleConditions(conditions, rangeField, ListField, addressField);
    const data = await collection.findMany({
      where: query,
      orderBy: orderByField,
      take,
      skip,
    });

    const totalCount = await collection.count({ where: query });
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
      return { success: true, data: newData };
    } catch (error) {
      console.error('Error creating data:', error);
      return { success: false, error: 'Error creating data' };
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
      return { success: true };
    } catch (error) {
      console.error('Error creating users:', error);
      return { success: false, error: 'Error creating data' };
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
        where: { id },
      });

      return { success: true };
    } catch (error) {
      console.error('Error deleting data:', error);
      return { success: false, error: 'Error deleting data' };
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
      return { success: true };
    } catch (error) {
      console.error(error);
      return { success: false };
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
    return { take, skip };
  }

  /**
   * 
   * @param {{}} conditions 
   * @param {[]} rangeField 
   * @param {string []} ListField 
   * @param {string []} addressField 
   * @returns 
   */
  async handleConditions(conditions, rangeField, ListField, addressField) {
    const where = {};
    for (const key in conditions) {
      if (Object.prototype.hasOwnProperty.call(conditions, key)) {
        const value = conditions[key];
        if (addressField.includes(key)) {
          let addressFilter = [];
          // The key must be add quotes
          // {"city": "San Jose", "country": "United States", "province": "California"}
          for (let i = 0; i < value.length; i++) {
            const address = value[i];
            // city
            if (address['city']) {
              addressFilter.push({
                [key]: {
                  path: ['city'],
                  equals: address.city
                }
              })
            }
            // country
            if (address['country']) {
              addressFilter.push({
                [key]: {
                  path: ['country'],
                  equals: address.country
                }
              })
            }
            // province
            if (address['province']) {
              addressFilter.push({
                [key]: {
                  path: ['province'],
                  equals: address.province
                }
              })
            }
          }
          // Prisma sentence OR/AND, OR like ||, AND like &&
          where['OR'] = addressFilter
        } else if (Array.isArray(value)) {
          // if rangeField is DateTime, value's type is string and must be ISO8601String  e.g.: 1969-07-20T20:18:04.000Z
          if (rangeField.includes(key)) {
            where[key] = {
              gte: value[0],
              lte: value[1],
            };
          }
          else if (ListField.includes(key)) {
            where[key] = { hasSome: value };
          }
          else {
            // for database field not array type, but use array search
            where[key] = { in: value };
          }
        } else {
          where[key] = { equals: value };
        }
      }
    }
    return where;
  }

  /**
   * @param {string || {}} input 
   * @returns 
   */
  toSQLDateTime(input) {
    if (typeof input === "string") {
      const timestamp = Date.parse(input);
      if (!isNaN(timestamp)) {
        const date = new Date(timestamp);
        return date;
      } else {
        return null;
      }
    } else if (typeof input === "object" && input !== null && "_seconds" in input) {
      const seconds = input['_seconds'];
      const date = new Date(seconds * 1000);
      return date;
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

    const values = Array.from({ length }, (_, index) => `$${index + 1}`).join(', ');
    return `VALUES(${values})`;
  }
}



module.exports = new SqlUtil();
