import asyncio
import logging
from typing import AsyncIterable

import grpc
import retrieval_pb2
import retrieval_pb2_grpc
#----
import sys

#import per il db:
import mysql.connector as sql
import config

class RetrievalService(retrieval_pb2_grpc.RetrievalServiceServicer):
    def __init__(self) -> None:
        super().__init__()
        
        # Connect to mysql
        try:
            self.cnx = sql.connect( 
                user=config.user,
                password=config.password,
                host=config.host, 
                database=config.database)
            self.cursor = self.cnx.cursor()
        except sql.Error as e:
            print(f"Error connecting to Mysql DB: {e}")
            sys.exit(1)
        
        
    async def GetDickeyFuller(
            self, request: retrieval_pb2.RetrievalRequest,
            context: grpc.aio.ServicerContext) -> retrieval_pb2.RetrievalDickeyFuller:
        for result in self.cursor.execute(request.query):
            return result.fetchall()
    
    
    async def ListDecomposition(
            self, request:retrieval_pb2.RetrievalRequest,
            context: grpc.aio.ServicerContext) -> AsyncIterable[retrieval_pb2.RetrievalDecomposition]:
        for result in self.cursor.execute(request.query):
            yield result.fetchall()
            
            
    async def ListHodrickPrescott(
            self, request:retrieval_pb2.RetrievalRequest,
            context: grpc.aio.ServicerContext) -> AsyncIterable[retrieval_pb2.RetrievalHodrickPrescott]:
        for result in self.cursor.execute(request.query):
            yield result.fetchall()
            
            
    async def ListAutocorrelation(
            self, request:retrieval_pb2.RetrievalRequest,
            context: grpc.aio.ServicerContext) -> AsyncIterable[retrieval_pb2.RetrievalAutocorrelation]:
        for result in self.cursor.execute(request.query):
            yield result.fetchall()
            
            
    async def ListAggregates(
            self, request:retrieval_pb2.RetrievalRequest,
            context: grpc.aio.ServicerContext) -> AsyncIterable[retrieval_pb2.RetrievalAggregates]:
        for result in self.cursor.execute(request.query):
            yield result.fetchall() 



async def serve() -> None:
    server = grpc.aio.server()
    retrieval_pb2_grpc.add_RetrievalServiceServicer_to_server(RetrievalService(), server)
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())


