/* 
 * File:   seekable_device.h
 * Author: thek
 *
 * Created on den 24 februari 2010, 21:13
 */

#ifndef _SEEKABLE_DEVICE_H
#define	_SEEKABLE_DEVICE_H

#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/positioning.hpp>
#include <boost/iostreams/concepts.hpp>

namespace io = boost::iostreams;

namespace MySQL { namespace system {

class SeekableDevice : io::device<io::seekable> {
public:
    // Read up to n characters from the input
    // sequence into the buffer s, returning
    // the number of characters read, or -1
    // to indicate end-of-sequence.
  virtual std::streamsize read(char* s, std::streamsize n)=0;


    // Write up to n characters from the buffer
    // s to the output sequence, returning the
    // number of characters written
  virtual std::streamsize write(const char* s, std::streamsize n)=0;

    // Advances the read/write head by off characters,
    // returning the new position, where the offset is
    // calculated from:
    //  - the start of the sequence if way == ios_base::beg
    //  - the current position if way == ios_base::cur
    //  - the end of the sequence if way == ios_base::end
  virtual io::stream_offset seek(io::stream_offset off, std::ios_base::seekdir way)=0;
};
} } // end namespace MySQL::system


#endif	/* _SEEKABLE_DEVICE_H */

