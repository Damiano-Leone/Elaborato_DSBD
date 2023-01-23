import asyncio
import logging

import grpc
import retrieval_pb2
import retrieval_pb2_grpc



# Performs an unary call
async def get_dickey_fuller(stub: retrieval_pb2_grpc.RetrievalServiceStub,
                            request: retrieval_pb2.RetrievalRequest) -> None:
    result = await stub.GetDickeyFuller(request)
    if not result:
        print("No rows found.\n\n")
    else:
        print(f"Query found:\n {result.name} {result.partition} {result.test_statistic} {result.p_value} {result.is_stationary} {result.crit_value1} {result.crit_value5} {result.crit_value10}\n\n")
        

# Performs server-streaming calls
async def list_decomposition(stub: retrieval_pb2_grpc.RetrievalServiceStub,
                             request: retrieval_pb2.RetrievalRequest) -> None:
    results = stub.ListDecomposition(request)
    if not results:
        print("No rows found.\n\n")
    else:
        print("Rows found:\n")
        async for result in results:
            print(f" {result.name} {result.partition} {result.timestamp} {result.typology} {result.value}\n")
        print("\n")
        

async def list_hodrick_prescott(stub: retrieval_pb2_grpc.RetrievalServiceStub,
                             request: retrieval_pb2.RetrievalRequest) -> None:
    results = stub.ListHodrickPrescott(request)
    if not results:
        print("No rows found.\n\n")
    else:
        print("Rows found:\n")
        async for result in results:
            print(f" {result.name} {result.partition} {result.timestamp} {result.value}\n")
        print("\n")
        

async def list_autocorrelation(stub: retrieval_pb2_grpc.RetrievalServiceStub,
                             request: retrieval_pb2.RetrievalRequest) -> None:
    results = stub.ListAutocorrelation(request)
    if not results:
        print("No rows found.\n\n")
    else:
        print("Rows found:\n")
        async for result in results:
            print(f" {result.name} {result.partition} {result.value}\n")
        print("\n")
        
        
async def list_aggregates(stub: retrieval_pb2_grpc.RetrievalServiceStub,
                             request: retrieval_pb2.RetrievalRequest) -> None:
    results = stub.ListAggregates(request)
    if not results:
        print("No rows found.\n\n")
    else:
        print("Rows found:\n")
        async for result in results:
            print(f" {result.name} {result.partition} {result.range} {result.typology} {result.value} {result.predicted_value}\n")
        print("\n")


#Metriche:
#Metrica2: node_memory_MemFree_bytes
#Metrica3: node_memory_MemAvailable_bytes 
#Metrica4: node_filesystem_free_bytes(2-partition: /dev/sda2, tmpfs)
#Metrica5: node_filesystem_avail_bytes(2-partition: /dev/sda2, tmpfs)
#Metrica7: node_filefd_allocated

async def node_memory_MemFree_bytes(stub: retrieval_pb2_grpc.RetrievalServiceStub) -> None:
    print( "Metrica 1: node_memory_MemFree_bytes\n")
                
    request = retrieval_pb2.RetrievalRequest(query='select * from dickey_fuller where nome="memory_MemFree_bytes"')
    print("-------------- GetDickeyFuller --------------\n")
    await get_dickey_fuller(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from decomposition where nome="memory_MemFree_bytes"')
    print("-------------- ListDecomposition --------------\n")
    await list_decomposition(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from hodrick_prescott where nome="memory_MemFree_bytes"')
    print("-------------- ListHodrickPrescott --------------\n")
    await list_hodrick_prescott(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from autocorrelation where nome="memory_MemFree_bytes"')
    print("-------------- ListAutocorrelation --------------\n")
    await list_autocorrelation(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from aggregates where nome="memory_MemFree_bytes"')
    print("-------------- ListAggregates --------------\n")
    await list_aggregates(stub, request)
    
    print( "Fine stampa Metrica 1: node_memory_MemFree_bytes\n")


async def node_memory_MemAvailable_bytes(stub: retrieval_pb2_grpc.RetrievalServiceStub) -> None:
    print( "Metrica 2: node_memory_MemAvailable_bytes\n")
                
    request = retrieval_pb2.RetrievalRequest(query='select * from dickey_fuller where nome="memory_MemAvailable_bytes"')
    print("-------------- GetDickeyFuller --------------\n")
    await get_dickey_fuller(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from decomposition where nome="memory_MemAvailable_bytes"')
    print("-------------- ListDecomposition --------------\n")
    await list_decomposition(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from hodrick_prescott where nome="memory_MemAvailable_bytes"')
    print("-------------- ListHodrickPrescott --------------\n")
    await list_hodrick_prescott(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from autocorrelation where nome="memory_MemAvailable_bytes"')
    print("-------------- ListAutocorrelation --------------\n")
    await list_autocorrelation(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from aggregates where nome="memory_MemAvailable_bytes"')
    print("-------------- ListAggregates --------------\n")
    await list_aggregates(stub, request)
    
    print( "Fine stampa Metrica 2: node_memory_MemAvailable_bytes\n")
    
    
async def node_filesystem_free_bytes_dev_sda2(stub: retrieval_pb2_grpc.RetrievalServiceStub) -> None:
        print( "Metrica 3: node_filesystem_free_bytes\nPartizione: /dev/sda2\n")
                
        request = retrieval_pb2.RetrievalRequest(query='select * from dickey_fuller where nome="filesystem_free_bytes" and partizione="/dev/sda2"')
        print("-------------- GetDickeyFuller --------------\n")
        await get_dickey_fuller(stub, request)
                
        request = retrieval_pb2.RetrievalRequest(query='select * from decomposition where nome="filesystem_free_bytes" and partizione="/dev/sda2"')
        print("-------------- ListDecomposition --------------\n")
        await list_decomposition(stub, request)
                
        request = retrieval_pb2.RetrievalRequest(query='select * from hodrick_prescott where nome="filesystem_free_bytes" and partizione="/dev/sda2"')
        print("-------------- ListHodrickPrescott --------------\n")
        await list_hodrick_prescott(stub, request)
                
        request = retrieval_pb2.RetrievalRequest(query='select * from autocorrelation where nome="filesystem_free_bytes" and partizione="/dev/sda2"')
        print("-------------- ListAutocorrelation --------------\n")
        await list_autocorrelation(stub, request)
                
        request = retrieval_pb2.RetrievalRequest(query='select * from aggregates where nome="filesystem_free_bytes" and partizione="/dev/sda2"')
        print("-------------- ListAggregates --------------\n")
        await list_aggregates(stub, request)
        
        print( "Fine stampa Metrica 3: node_filesystem_free_bytes\nPartizione: /dev/sda2\n")


async def node_filesystem_free_bytes_tmpfs(stub: retrieval_pb2_grpc.RetrievalServiceStub) -> None:
        print( "Metrica 3: node_filesystem_free_bytes\nPartizione: tmpfs\n")
                
        request = retrieval_pb2.RetrievalRequest(query='select * from dickey_fuller where nome="filesystem_free_bytes" and partizione="tmpfs"')
        print("-------------- GetDickeyFuller --------------\n")
        await get_dickey_fuller(stub, request)
                
        request = retrieval_pb2.RetrievalRequest(query='select * from decomposition where nome="filesystem_free_bytes" and partizione="tmpfs"')
        print("-------------- ListDecomposition --------------\n")
        await list_decomposition(stub, request)
                
        request = retrieval_pb2.RetrievalRequest(query='select * from hodrick_prescott where nome="filesystem_free_bytes" and partizione="tmpfs"')
        print("-------------- ListHodrickPrescott --------------\n")
        await list_hodrick_prescott(stub, request)
                
        request = retrieval_pb2.RetrievalRequest(query='select * from autocorrelation where nome="filesystem_free_bytes" and partizione="tmpfs"')
        print("-------------- ListAutocorrelation --------------\n")
        await list_autocorrelation(stub, request)
                
        request = retrieval_pb2.RetrievalRequest(query='select * from aggregates where nome="filesystem_free_bytes" and partizione="tmpfs"')
        print("-------------- ListAggregates --------------\n")
        await list_aggregates(stub, request)
        
        print( "Fine stampa Metrica 3: node_filesystem_free_bytes\nPartizione: tmpfs\n")


async def node_filesystem_avail_bytes_dev_sda2(stub: retrieval_pb2_grpc.RetrievalServiceStub) -> None:
    print( "Metrica 4: node_filesystem_avail_bytes\nPartizione: /dev/sda2\n")
                
    request = retrieval_pb2.RetrievalRequest(query='select * from dickey_fuller where nome="filesystem_avail_bytes" and partizione="/dev/sda2"')
    print("-------------- GetDickeyFuller --------------\n")
    await get_dickey_fuller(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from decomposition where nome="filesystem_avail_bytes" and partizione="/dev/sda2"')
    print("-------------- ListDecomposition --------------\n")
    await list_decomposition(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from hodrick_prescott where nome="filesystem_avail_bytes" and partizione="/dev/sda2"')
    print("-------------- ListHodrickPrescott --------------\n")
    await list_hodrick_prescott(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from autocorrelation where nome="filesystem_avail_bytes" and partizione="/dev/sda2"')
    print("-------------- ListAutocorrelation --------------\n")
    await list_autocorrelation(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from aggregates where nome="filesystem_avail_bytes" and partizione="/dev/sda2"')
    print("-------------- ListAggregates --------------\n")
    await list_aggregates(stub, request)
    
    print( "Fine stampa Metrica 4: node_filesystem_avail_bytes\nPartizione: /dev/sda2\n")


async def node_filesystem_avail_bytes_tmpfs(stub: retrieval_pb2_grpc.RetrievalServiceStub) -> None:
    print( "Metrica 4: node_filesystem_avail_bytes\nPartizione: tmpfs\n")
                
    request = retrieval_pb2.RetrievalRequest(query='select * from dickey_fuller where nome="filesystem_avail_bytes" and partizione="tmpfs"')
    print("-------------- GetDickeyFuller --------------\n")
    await get_dickey_fuller(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from decomposition where nome="filesystem_avail_bytes" and partizione="tmpfs"')
    print("-------------- ListDecomposition --------------\n")
    await list_decomposition(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from hodrick_prescott where nome="filesystem_avail_bytes" and partizione="tmpfs"')
    print("-------------- ListHodrickPrescott --------------\n")
    await list_hodrick_prescott(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from autocorrelation where nome="filesystem_avail_bytes" and partizione="tmpfs"')
    print("-------------- ListAutocorrelation --------------\n")
    await list_autocorrelation(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from aggregates where nome="filesystem_avail_bytes" and partizione="tmpfs"')
    print("-------------- ListAggregates --------------\n")
    await list_aggregates(stub, request)
    
    print( "Fine stampa Metrica 4: node_filesystem_avail_bytes\nPartizione: tmpfs\n")


async def node_filefd_allocated(stub: retrieval_pb2_grpc.RetrievalServiceStub) -> None:
    print( "Metrica 5: node_filefd_allocated\n")
                
    request = retrieval_pb2.RetrievalRequest(query='select * from dickey_fuller where nome="filefd_allocated"')
    print("-------------- GetDickeyFuller --------------\n")
    await get_dickey_fuller(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from decomposition where nome="filefd_allocated"')
    print("-------------- ListDecomposition --------------\n")
    await list_decomposition(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from hodrick_prescott where nome="filefd_allocated"')
    print("-------------- ListHodrickPrescott --------------\n")
    await list_hodrick_prescott(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from autocorrelation where nome="filefd_allocated"')
    print("-------------- ListAutocorrelation --------------\n")
    await list_autocorrelation(stub, request)
                
    request = retrieval_pb2.RetrievalRequest(query='select * from aggregates where nome="filefd_allocated"')
    print("-------------- ListAggregates --------------\n")
    await list_aggregates(stub, request)
    
    print( "Fine stampa Metrica 5: node_filefd_allocated\n")
    


async def run() -> None:
    print("Sending...")
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = retrieval_pb2_grpc.RetrievalServiceStub(channel)
    
    while(True):
        user_input = int(input("Inserisci un numero intero corrispondente a una metrica:\n\n1-node_memory_MemFree_bytes\n2-node_memory_MemAvailable_bytes\n3-node_filesystem_free_bytes\n4-node_filesystem_avail_bytes\n5-node_filefd_allocated\n6-Richiedi i dati relativi a tutte le metriche\n>>"))

        match user_input:

            case 1:
                await node_memory_MemFree_bytes(stub)
                
            case 2:
                await node_memory_MemAvailable_bytes(stub)
                
            case 3:
                await node_filesystem_free_bytes_dev_sda2(stub)
                await node_filesystem_free_bytes_tmpfs(stub)
                
            case 4:
                await node_filesystem_avail_bytes_dev_sda2(stub)
                await node_filesystem_avail_bytes_tmpfs(stub)
                
            case 5:
                await node_filefd_allocated(stub)
                
            case 6:
                await node_memory_MemFree_bytes(stub)
                await node_memory_MemAvailable_bytes(stub)
                await node_filesystem_free_bytes_dev_sda2(stub)
                await node_filesystem_free_bytes_tmpfs(stub)
                await node_filesystem_avail_bytes_dev_sda2(stub)
                await node_filesystem_avail_bytes_tmpfs(stub)
                await node_filefd_allocated(stub)
                
            case _:
                print( "Errore! Inserire un numero intero compreso tra 1 e 6.\n")


#Qualora si dovesse estendere il microservizio per fare anche altro parallelamente oltre alle richieste,
#inserire il codice all'interno di other().
async def other():
    for i in range(25):
        await asyncio.sleep(1)
        print(i)

async def main():
    await asyncio.gather(other(),run())

if __name__ == '__main__':
    logging.basicConfig()
    asyncio.run(main())
