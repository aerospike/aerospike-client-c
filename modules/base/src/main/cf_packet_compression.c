/*
 * Copyright 2013 Aerospike. All rights reserved.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <zlib.h>
#include "citrusleaf/cf_proto.h"
#include "citrusleaf/cf_log_internal.h"
#include <arpa/inet.h>

#define STACK_BUF_SZ (1024 * 16)

#define COMPRESSION_ZLIB 1

/* 
 * Function to compress the given data
 */
int
cf_compress(int argc, uint8_t *argv[])
{
	/* Expected arguments
 	 * 1. Type of compression
 	 * 	1 for zlib
 	 * 2. Length of buffer to be compressed - mandatory
 	 * 3. Pointer to buffer to be compressed - mandatory
 	 * 4. Length of buffer to hold compressed data - mandatory
 	 * 5. Pointer to buffer to hold compressed data - mandatory
 	 * 6. Compression level - Optional, default Z_DEFAULT_COMPRESSION
 	 */
	#define MANDATORY_NO_ARGUMENTS 5
	int compression_type;
	uint8_t *buf;
	size_t *buf_len;
	uint8_t *out_buf;
	size_t *out_buf_len;
	int compression_level;
	int ret_value = 0;
	
	cf_debug("In cf_compress");

	if (argc < MANDATORY_NO_ARGUMENTS)
	{
		// Insufficient arguments
		cf_debug("cf_compress : In sufficient arguments\n");
		cf_debug("Returned cf_compress : -1");
		return -1;
	}
		
	compression_type = *argv[0];
	buf_len = (size_t *) argv[1];
	buf = argv[2];
	out_buf_len = (size_t *) argv[3];
	out_buf = argv[4];

	compression_level = (argc > MANDATORY_NO_ARGUMENTS) ? (*argv[MANDATORY_NO_ARGUMENTS + 1]) : Z_DEFAULT_COMPRESSION;

	switch (compression_type)
	{
		case COMPRESSION_ZLIB:
			// zlib api to compress the data
			ret_value = compress2(out_buf, out_buf_len, buf, *buf_len, compression_level);
			break;
	}
	cf_debug("Returned cf_compress : %d", ret_value);
	return ret_value;
}

/* 
 * Function to create packet to send compressed data.
 * Packet :  Header - Original size of message - Compressed message.
 * Input : buf - Pointer to data to be compressed. - Input
 * 	   buf_sz - Size of the data to be compressed. - Input
 * 	   compressed_packet : Pointer holding address of compressed packet. - Output
 * 	   compressed_packet_sz : Size of the compressed packet. - Output
 */
int
cf_packet_compression(uint8_t *buf, size_t buf_sz, uint8_t **compressed_packet, size_t *compressed_packet_sz)
{
	uint8_t *tmp_buf;
	uint8_t wr_stack_buf[STACK_BUF_SZ];
	uint8_t *wr_buf = wr_stack_buf;
	size_t  wr_buf_sz = sizeof(wr_stack_buf);
	cf_debug("In cf_packet_compression");

	/* Compress the data using client API for compression.
	 * Expected arguments
 	 * 1. Type of compression
 	 * 	1 for zlib
 	 * 2. Length of buffer to be compressed - mandatory
 	 * 3. Pointer to buffer to be compressed - mandatory
 	 * 4. Length of buffer to hold compressed data - mandatory
 	 * 5. Pointer to buffer to hold compressed data - mandatory
 	 * 6. Compression level - Optional, default Z_DEFAULT_COMPRESSION
 	 */
 	uint8_t *argv[5];
	int argc = 5;
	int compression_type = COMPRESSION_ZLIB;
	argv[0] = (uint8_t *)&compression_type;	
	argv[1] = (uint8_t *)&buf_sz;	
	argv[2] = buf;	
	argv[3] = (uint8_t *)&wr_buf_sz;	
	argv[4] = wr_buf;	

	if (cf_compress(argc, argv))
	{
		compressed_packet = NULL;
		compressed_packet_sz = 0;
		cf_debug("Returned cf_packet_compression : -1");
		return -1;
	}
	
	// Allocate buffer to hold new packet
	*compressed_packet_sz = sizeof(cl_comp_proto) + wr_buf_sz; 
	*compressed_packet = (uint8_t *)calloc (*compressed_packet_sz, 1);

	if(!*compressed_packet)
	{
		cf_debug("cf_packet_compression : failed to allocte memory");
		cf_debug("Returned cf_packet_compression : -1");
		return -1;
	}
	// Construct the packet for compressed data.
	cl_comp_proto *cl_comp_protop = (cl_comp_proto *) *compressed_packet;
	cl_comp_protop->proto.version = CL_PROTO_VERSION;
	cl_comp_protop->proto.type = CL_PROTO_TYPE_CL_MSG_COMPRESSED;
	cl_comp_protop->proto.sz = *compressed_packet_sz - 8;
	cl_proto *proto = (cl_proto *) *compressed_packet;
	cl_proto_swap_to_be(proto);
	cl_comp_protop->org_sz = buf_sz;

	tmp_buf = *compressed_packet +  sizeof(cl_comp_proto);
	memcpy (tmp_buf, wr_buf, wr_buf_sz);

	cf_debug("Returned cf_packet_compression : 0");
	return 0;
}

/*      
 * Function to decompress the given data
 */
int
cf_decompress(int argc, uint8_t **argv)
{
	/* Expected arguments
	 * 1. Type of compression
 	 *  1 for zlib
	 * 2. Length of buffer to be decompressed - mandatory
	 * 3. Pointer to buffer to be decompressed - mandatory
	 * 4. Length of buffer to hold decompressed data - mandatory
	 * 5. Pointer to buffer to hold decompressed data - mandatory
	 */
    #define MANDATORY_NO_ARGUMENTS 5
    int compression_type;
    size_t *buf_len;
    uint8_t *buf;
    size_t *out_buf_len;
    uint8_t *out_buf;
    int ret_value = 0;

    cf_debug ("In cf_decompress");

    compression_type = *argv[0];
    buf_len = (size_t *)argv[1];
    buf = argv[2];
    out_buf_len = (size_t *)argv[3];
    out_buf = argv[4];

    switch (compression_type)
    {
        case COMPRESSION_ZLIB:
            // zlib api to decompress the data
			ret_value = uncompress(out_buf, out_buf_len, buf, *buf_len);
			break;
	}
	cf_debug ("Returned cf_decompress : %d", ret_value);
	return ret_value;
}

/* 
 * Function to decompress packet from CL_PROTO_TYPE_CL_MSG_COMPRESSED packet
 * Received packet :  Header - Original size of message - Compressed message
 * Input : buf - Pointer to packet to be decompressed. - Input
 *         decompressed_packet - Decompressed packet received in buf. - Input/Output
 */
int
cf_packet_decompression(uint8_t *buf, uint8_t **decompressed_packet)
{
    int ret_value;
    size_t decompressed_packet_sz;
    size_t buf_sz;

    cl_comp_proto *cl_comp_protop = (cl_comp_proto *) buf;

    cf_debug ("In cf_packet_decompression");

    if (cl_comp_protop->proto.type != CL_PROTO_TYPE_CL_MSG_COMPRESSED)
	{
        cf_debug ("cf_packet_decompression : Invalid input data");
        cf_debug ("Returned cf_packet_decompression : -1");
        return -1;
	}

    decompressed_packet_sz = cl_comp_protop->org_sz;
	buf_sz = cl_comp_protop->proto.sz - 8;
    buf = buf + sizeof (cl_comp_proto);

    *decompressed_packet = (uint8_t *)calloc (decompressed_packet_sz, 1);

    /* Call client API to decompress data
	 * Expected arguments
	 * 1. Type of compression
	 *  1 for zlib
 	 * 2. Length of buffer to be decompressed - mandatory
	 * 3. Pointer to buffer to be decompressed - mandatory
	 * 4. Length of buffer to hold decompressed data - mandatory
	 * 5. Pointer to buffer to hold decompressed data - mandatory
	 */
    uint8_t *argv[5];
    int argc = 5;
    int compression_type = COMPRESSION_ZLIB;
    argv[0] = (uint8_t *)&compression_type;
    argv[1] = (uint8_t *)&buf_sz;
    argv[2] = buf;
    argv[3] = (uint8_t *)&decompressed_packet_sz;
    argv[4] = *decompressed_packet;

    ret_value = cf_decompress(argc, argv);
    if (ret_value)
    {
        free (decompressed_packet);
        decompressed_packet = NULL;
    }
    cf_debug ("Returned cf_packet_decompression : %d", ret_value);
    return (ret_value);
}
