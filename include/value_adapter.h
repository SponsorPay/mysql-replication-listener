/* 
 * File:   value_adapter.h
 * Author: thek
 *
 * Created on den 6 april 2010, 10:10
 */

#ifndef _VALUE_ADAPTER_H
#define	_VALUE_ADAPTER_H

#include <boost/cstdint.hpp>
#include "protocol.h"
#include <boost/any.hpp>
#include <iostream>

using namespace MySQL;
namespace MySQL {

/**
 This helper function calculates the size in bytes of a particular field in a
 row type event as defined by the field_ptr and metadata_ptr arguments.
 @param column_type Field type code
 @param field_ptr The field data
 @param metadata_ptr The field metadata

 @note We need the actual field data because the string field size is not
 part of the meta data. :(

 @return The size in bytes of a particular field
*/
int calc_field_size(unsigned char column_type, unsigned char *field_ptr,
                    boost::uint32_t metadata);


/**
 * A value object class which encapsluate a tuple (value type, metadata, storage)
 * and provide for views to this storage through a well defined interface.
 *
 * Can be used with a Converter to convert between different Values.
 */
class Value
{
public:
    Value(enum system::enum_field_types type, boost::uint32_t metadata, char *storage) :
      m_type(type), m_storage(storage), m_metadata(metadata), m_is_null(false)
    {
      m_size= calc_field_size((unsigned char)type,
                              (unsigned char*)storage,
                              metadata);
      //std::cout << "TYPE: " << type << " SIZE: " << m_size << std::endl;
    };

    Value()
    {
      m_size= 0;
      m_storage= 0;
      m_metadata= 0;
      m_is_null= false;
    }
    
    /**
     * Copy constructor
     */
    Value(Value& val);
    Value(const Value& val);

    Value &operator=(const Value &val);
    bool operator==(Value &val);
    bool operator!=(Value &val);

    ~Value() {}

    void is_null(bool s) { m_is_null= s; }
    bool is_null(void) { return m_is_null; }
    
    char *storage() { return m_storage; }

    /**
     * Get the size in bytes. of the entire storage (any metadata part +
     * atual data)
     */
    size_t size() { return m_size; }
    enum system::enum_field_types type() { return m_type; }
    boost::uint32_t metadata() { return m_metadata; }
    
    /**
     * Returns the integer representation of a storage of a pre-specified
     * type.
     */
    boost::int32_t sql_integer();

    /**
     * Returns the integer representation of a storage of pre-specified
     * type.
     */
    boost::int64_t sql_bigint();

    /**
     * Returns the integer representation of a storage of pre-specified
     * type.
     */
    boost::int8_t sql_tinyint();

    /**
     * Returns the integer representation of a storage of pre-specified
     * type.
     */
    boost::int16_t sql_smallint();

    /**
     * Returns a pointer to the character data of a string type stored
     * in the pre-defined storage.
     * @note The position is an offset of the storage pointer determined
     * by the metadata and type.
     *
     * @param[out] size The size in bytes of the character string.
     * 
     */
    char *sql_string_ptr(unsigned long &size);

    /**
     * Returns a pointer to the byte data of a blob type stored in the pre-
     * defined storage.
     * @note The position is an offset of the storage pointer determined
     * by the metadata and type.
     *
     * @param[out] size The size in bytes of the blob data.
     */
    char *sql_blob(unsigned long &size);

    double sql_decimal() { return 0.0; }
    float sql_float();
    double sql_double();
    long sql_timestamp() { return sql_integer(); }


//			MYSQL_TYPE_DATE,   MYSQL_TYPE_TIME,
//			MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
//			MYSQL_TYPE_NEWDATE
//			MYSQL_TYPE_BIT,
//                      MYSQL_TYPE_NEWDECIMAL=246,
//			MYSQL_TYPE_ENUM=247,
//			MYSQL_TYPE_SET=248,
//			MYSQL_TYPE_TINY_BLOB=249,
//			MYSQL_TYPE_MEDIUM_BLOB=250,
//			MYSQL_TYPE_LONG_BLOB=251,
//			MYSQL_TYPE_BLOB=252,
//			MYSQL_TYPE_VAR_STRING=253,
//			MYSQL_TYPE_STRING=254,

private:
    enum system::enum_field_types m_type;
    size_t m_size;
    char *m_storage;
    boost::uint32_t m_metadata;
    bool m_is_null;
};

class Converter
{
public:
    Converter() {}
    ~Converter() {}

    /**
     * Converts and copies the sql value to a std::string object.
     * @param[out] str The target string
     * @param[in] val The value object to be converted
     */
    void to_string(std::string &str, Value &val);

    /**
     * Converts and copies the sql value to a long integer.
     * @param[out] out The target variable
     * @param[in] val The value object to be converted
     */
    void to_long(long &out, Value &val);

    /**
     * Converts and copies the sql value to a floating point number.
     * @param[out] out The target variable
     * @param[in] val The value object to be converted
     */
    void to_float(float &out, Value &val);
};


} // end namespace MySQL
#endif	/* _VALUE_ADAPTER_H */

