using System;
using System.Collections.Generic;
using System.Net.Sockets;
using Cross;
using NetSocket.Request;
using Ionic.Zlib;
using Google.Protobuf;
using System.IO;
using Assets.Core.Net;

namespace NetSocket
{
    public class Connection
    {
        const int buffSize = 0xFFFF;
        const int readSize = 4096;
        public const int HEAD_LEN = 4;

        private Queue<byte[]> msgList = new Queue<byte[]>();
        private TcpClient tcp;
        private bool isConnected;
        private byte[] readData = new byte[0xFFFF];
        private int offSet = 0;
        private int leftRecieveDataLength = 0;
        private int msgLength = 0;

        private byte[] _tmpBytes;
        private byte[] _edataBytes;

        //委托
        private ConnectionDelegate _OnConnect;
        private OnTypeDataDelegate _OnTypeDataReceive;
        private OnFullTypeDataDelegate _OnFullTypeDataReceive;
        private OnRpcDataDelegate _OnRpcDataReceive;
        private DisConnectionDelegate _OnDisConnect;
        private OnErrorDelegate _OnError;
        private OnDataWriteDelegate _OnDataWrite;


        private uint last_server_tm_ms = 0;
        private static uint min_last_server_tm_ms = 0;

        private int last_hbs_time_ms = 0;
		private int last_hbr_time_ms = 0;
		private int latency_ms = 0;

        // ping 值最小的情况下获取的服务器时间记录
		private int min_latency_ms = 0;
		private static int min_last_hbr_time_ms = 0;
        public static long server_tm_diff_s = 0;
		private uint hb_interval_val = 2000;
	   
        #region  Properties
        public ConnectionDelegate OnConnect
        {
            get
            {
                return this._OnConnect;
            }
            set
            {
                this._OnConnect = value;
            }
        }

        public OnTypeDataDelegate OnTypeDataReceive
        {
            get
            {
                return this._OnTypeDataReceive;
            }
            set
            {
                this._OnTypeDataReceive = value;
            }
        }

        public OnFullTypeDataDelegate OnFullTypeDataReceive
        {
            get
            {
                return this._OnFullTypeDataReceive;
            }
            set
            {
                this._OnFullTypeDataReceive = value;
            }
        }

        public OnRpcDataDelegate OnRpcDataReceive
        {
            get
            {
                return this._OnRpcDataReceive;
            }
            set
            {
                this._OnRpcDataReceive = value;
            }
        }

        public OnDataWriteDelegate OnDataWrite
        {
            get
            {
                return this._OnDataWrite;
            }
            set
            {
                this._OnDataWrite = value;
            }
        }

        public DisConnectionDelegate OnDisconnect
        {
            get
            {
                return this._OnDisConnect;
            }
            set
            {
                this._OnDisConnect = value;
            }
        }

        public OnErrorDelegate OnError
        {
            get
            {
                return this._OnError;
            }
            set
            {
                this._OnError = value;
            }
        }

        public TcpClient GetTCPClient
        {
            get
            {
                return this.tcp;
            }
        }
        #endregion


        public int recon;
        //开始连接
        public void Connect(string ipaddr, int port,int recon=0)
        {
            try
            {
                readData = new byte[buffSize];
                leftRecieveDataLength = 0;
                offSet = 0;
                tcp = new TcpClient();
                tcp.ReceiveBufferSize = 40960;
                tcp.SendBufferSize = 40960;
                tcp.Connect(ipaddr, port);
                this.recon = recon;
                isConnected = true;
                CallOnConnect();
                tcp.Client.BeginReceive(readData, 0, readSize, SocketFlags.None,DoRead, tcp);
            }
            catch (Exception ex)
            {
                this.HandleError(" Connect Error.      " + ex.ToString() + ex.StackTrace);
            }
        }

        //断开连接
        public void Disconnect()
        {
            try
            {
                this.isConnected = false;
                tcp.Close();

            }
            catch (Exception ex)
            {
                this.HandleError(" Please Stop server and try again.      " + ex.ToString() + ex.StackTrace);
            }

        }

        //连接成功后读取数据
        private void DoRead(IAsyncResult result)
        {
            //UnityEngine.Debug.Log("DoRead");
            if (isConnected == false)
            {
                //this.Of9888461c3300e3708c6973a119b08319.Trace("DoRead --->  Server is not active.  Please start server and try again.      ");
                return;
            }
            int length = 0;
            try
            {
                lock (tcp)
                {
                    if (tcp.Client != null)
                    {
                        length = tcp.Client.EndReceive(result);
                    }
                }
                if (length < 1)
                {
                    HandleError("Do Read Disconnected");
                    return;
                }
                //UnityEngine.Debug.Log("Do_read_length:"+length);
                //ReadBuffInfo(length);
                onData(length);
                lock (tcp)
                {
                    if (tcp.Client != null)
                    {
                        tcp.Client.BeginReceive(readData, offSet + leftRecieveDataLength, readSize, SocketFlags.None, new AsyncCallback(DoRead), tcp);
                    }
                }
            }
            catch (SocketException socketerror)
            {
                if (socketerror.SocketErrorCode != SocketError.ConnectionReset)
                {
                    string errorMsg = "Error reading msg from socket: " + socketerror.Message + " ,, " + socketerror.SocketErrorCode + " ,, " + socketerror.StackTrace;
                    HandleError(errorMsg);
                }
            }
            catch (Exception generalError)
            {
                string errorMsg = "General socket on read: " + generalError.Message + " ,, " + generalError.StackTrace;
               HandleError(errorMsg);
            }
      
        }
        private void onData(int length)
        {
            leftRecieveDataLength += length;
            MsgStream ms = new MsgStream();
            ms.setReadIndex(offSet);
            byte[] temp = null;
            while (leftRecieveDataLength > 0)
            {
                if (msgLength == 0)
                {
                    if (leftRecieveDataLength < HEAD_LEN)
                        break;
                    ms.Read(readData, ref msgLength);
                    leftRecieveDataLength -= 4;
                    offSet += 4;
                    if (msgLength == 0)
                        break;
                }
                if (leftRecieveDataLength < msgLength)
                    break;
                int msgid = 0;
                ms.Read(readData, ref msgid);
                msgLength -= 4;
                leftRecieveDataLength -= 4;
                offSet += 4;
                if(msgLength > 0)
                    ms.Read(readData, msgLength, ref temp);
                DispatchCmd(msgid, temp);
                leftRecieveDataLength -= msgLength;
                offSet += msgLength;
                msgLength = 0;
            }
        }

        private void DispatchCmd(int msgid, byte[] data)
        {
            data = decryptForDis(data);
        }
        private byte[] decryptForDis(byte[] data)
        {
            
            int buff;
            for (int i = 0; i < data.Length; i += 5)
            {
                if (i + 3 > data.Length-1) { break; }
                buff = data[i + 2];
                data[i + 2] = (byte)~data[i + 3];
                data[i + 3] = (byte)buff;
            }
            byte[] bytes = new byte[data.Length];
            data.CopyTo(bytes, 0);
            return bytes;
        }

        public void Write(byte[] msg)
        {
            try
            {
                lock (msgList)
                {
                    if (msgList.Count == 0)
                    {
                        msgList.Enqueue(msg);
                        tcp.Client.BeginSend(msg, 0, msg.Length, SocketFlags.None, new AsyncCallback(DoWrite), msg);
                    }
                    else
                    {
                        msgList.Enqueue(msg);
                    }
                }
            }
            catch (Exception ex)
            {
                string error = "Write exMsg on connection: " + ex.StackTrace + ex.Message + " " + ex.StackTrace;
                this.HandleError(error);
            }
        }

        private void DoWrite(IAsyncResult msg)
        {
            if (isConnected == false)
            {
                //this.log.Trace("DoWrite --->  Server is not active.  Please start server and try again.      ");
                return;
            }
            //UnityEngine.Debug.Log("DoWrite");
            SocketError socketError;
            byte[] bytes = (byte[])msg.AsyncState;
            int length = tcp.Client.EndSend(msg, out socketError);
            if (socketError == SocketError.Success)
            {
                try
                {
                    lock (msgList)
                    {
                        //通知
                        this.CallOnDataWrite(bytes);
                        //队列判断
                        if (msgList.Count > 0)
                        {
                            msgList.Dequeue();
                            if (msgList.Count > 0)
                            {
                                byte[] firstMsg = msgList.Peek();
                                tcp.Client.BeginSend(firstMsg, 0, firstMsg.Length, SocketFlags.None, new AsyncCallback(DoWrite), firstMsg);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    this.HandleError("Revc Successs Error:      " + ex.ToString() + ex.StackTrace);
                }
                return;
            }
            if ((socketError != SocketError.WouldBlock) && (socketError != SocketError.InProgress))
            {
                try
                {
                    lock (msgList)
                    {
                        if (msgList.Count > 0)
                        {
                            byte[] firstMsg = msgList.Peek();
                            tcp.Client.BeginSend(firstMsg, 0, firstMsg.Length, SocketFlags.None, new AsyncCallback(DoWrite), firstMsg);
                        }
                    }
                }
                catch (Exception ex)
                {
                    this.HandleError("SocketError.WouldBlock .      " + ex.ToString() + ex.StackTrace);
                }

                return;
            }


            try
            {
                throw new SocketException((int)socketError);
            }
            catch (SocketException ex)
            {
                this.HandleError(" DoWrite .      " + ex.ToString() + ex.StackTrace);
            }
        }

        public void HeartBeat()
        {
            if (isConnected)
            {
                last_hbs_time_ms = APP.Instance.timer.getTimer();
                MsgStream ms = new MsgStream();
                ms.WriteBytes((byte)Protocol_base._make_sgn_pkg_header(0x00));
                ms.WriteBytes((uint)(last_server_tm_ms + (last_hbs_time_ms - last_hbr_time_ms)));
                Write(ms.OutByte());
            }
        }
     
        //private void ReadBuffInfo(int length)
        //{
        //    //int count = 100;
        //    uint pkg_header = 0;
        //    byte pkg_header_tmp=new byte();
        //    uint pkg_header_flag;
        //    uint len;
        //    byte[] edata= null;
        //    byte[] tmp = null;
        //    leftRecieveDataLength += length;
        //    while (true)
        //    {
        //        if (leftRecieveDataLength <= 0)
        //            break;

        //        MsgStream ms = new MsgStream();
        //        ms.setReadIndex(offSet);
        //        if (NetworkManager.ClientHandle.decrypt != null)
        //        {
        //            readData = NetworkManager.ClientHandle.decrypt.Decrypt(0, readData);
        //        }
        //        ms.Read(readData, ref pkg_header_tmp);
        //        pkg_header = Convert.ToUInt16(pkg_header_tmp);
        //        pkg_header_flag = pkg_header & 0xc0;
        //        if (pkg_header_flag == 0x00) // sgn package
        //        {
        //            if (pkg_header == 0x00)
        //            {
        //                if (ms.bytesAvailable(offSet + leftRecieveDataLength) >= 4)
        //                {
        //                    //UnityEngine.Debug.Log("sgn read");
        //                    ms.Read(readData, ref last_server_tm_ms);
                          
        //                    last_hbr_time_ms = APP.Instance.timer.getTimer();
        //                    latency_ms = last_hbr_time_ms - last_hbs_time_ms;
        //                    if (min_last_server_tm_ms == 0)
        //                    {
        //                        min_last_server_tm_ms = last_server_tm_ms;
        //                        min_last_hbr_time_ms = last_hbr_time_ms;
        //                        min_latency_ms = latency_ms;
        //                    }

        //                    if (latency_ms < min_latency_ms)
        //                    {
        //                        // 取ping值最小的服务器时间记录作为服务器时间计算基础
        //                        min_last_server_tm_ms = last_server_tm_ms;
        //                        min_last_hbr_time_ms = last_hbr_time_ms;
        //                        min_latency_ms = latency_ms;
        //                        DebugTrace.Log("min_last_server_tm_ms:" + min_last_server_tm_ms);
        //                        DebugTrace.Log("min_last_hbr_time_ms:" + min_last_hbr_time_ms);
        //                        DebugTrace.Log("min_latency_ms:" + min_latency_ms);
        //                    }
        //                    offSet += 5;
        //                    leftRecieveDataLength -= 5;
        //                    if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                    {
        //                        Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                        offSet = 0;
        //                        if (leftRecieveDataLength <= 0)
        //                            break;
        //                    }
        //                }
        //                else
        //                {
        //                    if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                    {
        //                        Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                        offSet = 0;
        //                    }
        //                    break;
        //                }
        //            }
        //            else
        //            {
        //                if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                {
        //                    Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                    offSet = 0;
        //                }
        //                offSet = 0;
        //                break;
        //            }
        //        }
        //        else if (pkg_header_flag == 0x40)
        //        {
        //            if (ms.bytesAvailable(offSet+leftRecieveDataLength) >= 1)
        //            {
        //                //UnityEngine.Debug.Log("rpc read");

        //                byte b = new byte();
        //                len = ((pkg_header & 0x1f) << 8);
        //                ms.Read(readData, ref b);
        //                len |= b;
        //                int buffl = (int) len - 2;
        //                if (ms.bytesAvailable(offSet+leftRecieveDataLength) >= buffl)
        //                {
        //                    ms.Read(readData, offSet+2, (int)len-2, ref edata);
        //                    if (_is_pkg_compressed(pkg_header))
        //                    {
        //                        tmp = ZlibStream.UncompressBuffer(edata);
        //                        //DebugTrace.Log("rpc package is compressed");
        //                    }
        //                    else
        //                    {
        //                        tmp = edata;
        //                    }
        //                    //CallOnRpcData(Package.ins.unpack_rpc_package(tmp));
        //                    //Package.ins.unpack_rpc_package(tmp);
        //                    CallOnRpcData(tmp);
        //                    offSet += (int)len;
        //                    leftRecieveDataLength -= (int)len;
        //                    if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                    {
        //                        Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                        offSet = 0;
        //                    }
        //                }
        //                else
        //                {
        //                    if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                    {
        //                        Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                        offSet = 0;
        //                    }
        //                    break;
        //                }
        //            }
        //            else
        //            {
        //                if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                {
        //                    Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                    offSet = 0;
        //                }
        //                break;
        //            }
                   
        //        }
        //        else if (pkg_header_flag == 0x80) // type package
        //        {
        //            if (ms.bytesAvailable(offSet+leftRecieveDataLength) >= 3)
        //            {
        //                //DebugTrace.Log("type read");
        //                len =((pkg_header & 0x1f) << 24);
        //                byte b = new byte();
        //                ms.Read(readData, ref b);
        //                len |= b;
        //                ushort us = 0;
        //                ms.Read(readData, ref us);
        //                len |= (uint)(us << 8);
        //                int buffl = (int) len-4;
        //                if (ms.bytesAvailable(offSet + leftRecieveDataLength) >= buffl)
        //                {
                           
        //                    ms.Read(readData, offSet+4, (int)len-4, ref edata);
        //                    if (_is_pkg_compressed(pkg_header))
        //                    {
        //                        tmp = ZlibStream.UncompressBuffer(edata);
        //                        //DebugTrace.Log("type package is compressed");
        //                    }
        //                    else
        //                    {
        //                        tmp = edata;
        //                    }
        //                    CallOnTypeData(Package.ins.unpack_type_pkg(tmp));
        //                    offSet += (int)len;
        //                    leftRecieveDataLength -= (int)len;
        //                    if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                    {
        //                        Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                        offSet = 0;
        //                    }
        //                }
        //                else
        //                {
        //                    if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                    {
        //                        Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                        offSet = 0;
        //                    }
        //                    break;
        //                }
        //            }
        //            else
        //            {
        //                if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                {
        //                    Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                    offSet = 0;
        //                }
        //                break;
        //            }
                   
        //        }
        //        else if (pkg_header_flag == 0xc0)
        //        {
        //            //UnityEngine.Debug.Log("full type read");
        //            if (ms.bytesAvailable(offSet+leftRecieveDataLength) > 3)
        //            {
        //                len = ((pkg_header & 0x1f) << 24);
        //                byte b = 0;
        //                ms.Read(readData, ref b);
        //                len |= b;

        //                ushort us = 0;
        //                ms.Read(readData, ref us);
        //                len |= (uint)(us << 8);
        //                int buffl = (int)len - 4;
        //                if (ms.bytesAvailable(offSet + leftRecieveDataLength) >= buffl)
        //                {
        //                    ms.Read(readData, offSet+4, (int)len-4, ref edata);
        //                    if (_is_pkg_compressed(pkg_header))
        //                    {
        //                        tmp = ZlibStream.UncompressBuffer(edata);
        //                        //DebugTrace.Log("full type package is compressed");
        //                    }
        //                    else
        //                    {
        //                        tmp = edata;
        //                    }
        //                    CallOnFullTypeData(Package.ins.unpack_full_type_pkg(tmp));
        //                    offSet += (int)len;
        //                    leftRecieveDataLength -= (int)len;
        //                    if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                    {
        //                        Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                        offSet = 0;
        //                    }
        //                }
        //                else
        //                {
        //                    if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                    {
        //                        Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                        offSet = 0;
        //                    }
        //                    break;
        //                }
        //            }
        //            else
        //            {
        //                if (buffSize - offSet - leftRecieveDataLength < readSize)
        //                {
        //                    Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                    offSet = 0;
        //                }
        //                break;
        //            }
        //        }
        //        else
        //        {
        //            if (buffSize - offSet - leftRecieveDataLength < readSize)
        //            {
        //                Buffer.BlockCopy(readData, offSet, readData, 0, leftRecieveDataLength);
        //                offSet = 0;
        //            }
        //            break;
        //        }
               
        //    }
        //}

        private bool _is_pkg_compressed(uint header)
		{
			return Convert.ToBoolean(header & (0x1<<5));
		}
        private void HandleError(string error)
        {
            HandleError(error, true);
        }
        private void HandleError(string error, bool isDisConnect)
        {
            lock (msgList)
            {
                msgList.Clear();
            }
            //this.log.Error(error);
            this.CallOnError(error);

            if (isDisConnect == true)
            {
                this.isConnected = false;
                tcp.Close();
                //响应中断
                CallOnDisconnect();
                //this.log.Trace(" Stop server");
            }
        }

        private void CallOnDataWrite(byte[] data)
        {
            if (this._OnDataWrite != null)
            {
                this._OnDataWrite(data);
            }
        }

        private void HandleDisconnection()
        {

            this.CallOnDisconnect();

        }

        private void CallOnTypeData(Dictionary<uint, Variant> binData)
        {
            if (this._OnTypeDataReceive != null)
            {
                this._OnTypeDataReceive(binData);
            }
        }

        private void CallOnFullTypeData(Dictionary<uint, Cross.Variant> binData)
        {
            if (this._OnFullTypeDataReceive != null)
            {
                this._OnFullTypeDataReceive(binData);
            }
        }

        //private void CallOnRpcData(Dictionary<uint, object> binData)
        //{
        //    if (this._OnRpcDataReceive != null)
        //    {
        //        this._OnRpcDataReceive(binData);
        //    }
        //}
        private void CallOnRpcData(byte[] msg)
        {
            if (this._OnRpcDataReceive != null)
            {
                this._OnRpcDataReceive(msg);
            }
        }

        private void CallOnConnect()
        {
            if (this._OnConnect != null)
            {
                this._OnConnect();
            }
        }

        private void CallOnDisconnect()
        {
            if (this._OnDisConnect != null)
            {
                this._OnDisConnect();
            }
        }

        private void CallOnError(string error)
        {
            if (this._OnError != null)
            {
                this._OnError(error);
            }
        }

        /**
		 * 获取到目前服务器的毫秒级对表时间,因受延迟影响,该值为估算值,精度为 +-延迟时间.
		 * @return 以毫秒为单位.
		 */
		public static long cur_server_ms_tm
		{
            get
            {
                if (APP.Instance != null)
                    return (uint)(min_last_server_tm_ms + ((int)(APP.Instance.timer.getTimer()) - min_last_hbr_time_ms));
                else
                    return 0;
            }
            
		}

        public int serverPing
        {
            get { return latency_ms; }
        }

        /**
		 * 获取到目前服务器的UNIX时间戳对表时间,因受延迟影响,该值为估算值,精度为 +-延迟时间.
		 * @return 以毫秒为单位.
		 */
		public static long cur_server_tm
		{
		    get
		    {
			    return APP.Instance.timer.getTime() + server_tm_diff_s;
		    }
		   
		}
    }
}

