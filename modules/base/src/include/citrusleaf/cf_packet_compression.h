/*
 * Copyright 2013 Aerospike. All rights reserved.
 */

/*      
 * Function to decompress the given data
 * Expected arguments
 * 1. Type of compression
 *  1 for zlib
 * 2. Length of buffer to be decompressed - mandatory
 * 3. Pointer to buffer to be decompressed - mandatory
 * 4. Length of buffer to hold decompressed data - mandatory
 * 5. Pointer to buffer to hold decompressed data - mandatory
  */
int
cf_decompress(int argc, uint8_t **argv);

/* 
 * Function to get back decompressed packet from CL_PROTO_TYPE_CL_MSG_COMPRESSED packet
 * Packet :  Header - Original size of message - Compressed message
 * Input : buf - Pointer to CL_PROTO_TYPE_CL_MSG_COMPRESSED packet. - Input
 *         decompressed_packet - Pointer holding address of decompressed packet. - Output
 */
int 
cf_packet_decompression(uint8_t *buf, uint8_t *decompressed_packet);

/* 
 * Function to compress the given data
 * Expected arguments
 * 1. Type of compression
 *  1 for zlib
 * 2. Length of buffer to be compressed - mandatory
 * 3. Pointer to buffer to be compressed - mandatory
 * 4. Length of buffer to hold compressed data - mandatory
 * 5. Pointer to buffer to hold compressed data - mandatory
 * 6. Compression level - Optional, default Z_DEFAULT_COMPRESSION
 *                                          Z_NO_COMPRESSION         0
 *                                          Z_BEST_SPEED             1
 *                                          Z_BEST_COMPRESSION       9
 *                                          Z_DEFAULT_COMPRESSION  (-1)
 */
int
cf_compress(int argc, uint8_t *argv[]);

/* 
 * Function to create packet to send compressed data.
 * Packet :  Header - Original size of message - Compressed message.
 * Input : buf - Pointer to data to be compressed. - Input
 *     buf_sz - Size of the data to be compressed. - Input
 *     compressed_packet : Pointer holding address of compressed packet. - Output
 *     compressed_packet_sz : Size of the compressed packet. - Output
 */
int
cf_packet_compression(uint8_t *buf, size_t buf_sz, uint8_t **compressed_packet, size_t *compressed_packet_sz);
