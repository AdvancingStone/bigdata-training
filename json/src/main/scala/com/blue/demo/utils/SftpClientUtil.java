package com.blue.demo.utils;

import com.jcraft.jsch.*;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class SftpClientUtil {

    /**
     * 获取远程sftp服务器的Session
     *
     * @param ip   host
     * @param user 用户名
     * @param pwd  密码
     * @param port 端口
     */
    public static Session getSftpSession(String ip, String user, String pwd, int port) {
        Session session = null;
        JSch jsch = new JSch();
        try {
            if (port <= 0) {
                // 连接服务器，采用默认端口
                session = jsch.getSession(user, ip);
            } else {
                // 采用指定的端口连接服务器
                session = jsch.getSession(user, ip, port);
            }

            // 如果服务器连接不上，则抛出异常
            if (session == null) {
                throw new Exception("session is null");
            }

            // 设置登陆主机的密码
            session.setPassword(pwd);// 设置密码
            // 设置第一次登陆的时候提示，可选值：(ask | yes | no)
            session.setConfig("StrictHostKeyChecking", "no");
            // 设置登陆超时时间
            session.connect(300000);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return session;
    }


    /**
     * 从远程sftp服务器下载文件到本地
     *
     * @param ip                    host
     * @param user                  用户名
     * @param pwd                   密码
     * @param port                  端口
     * @param srcAbsolutePath       源文件绝对路径
     * @param destLocalAbsolutePath 本地目标文件绝对路径
     */

    public static boolean sshSftpDownLoadFile(String ip, String user, String pwd, int port, String srcAbsolutePath, String destLocalAbsolutePath) {
        Session session = getSftpSession(ip, user, pwd, port);
        InputStream stream = null;
        FileOutputStream fos = null;
        boolean bool = true;
        try {
            stream = SftpClientUtil.readFile(session, srcAbsolutePath);
            fos = new FileOutputStream(destLocalAbsolutePath);
            byte[] buffer = new byte[1024];
            int size = 0;
            while (size != -1) {
                fos.write(buffer, 0, size);
                size = stream.read(buffer);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            bool = false;
        } catch (IOException e) {
            e.printStackTrace();
            bool = false;
        } catch (JSchException e) {
            e.printStackTrace();
            bool = false;
        } catch (SftpException e) {
            e.printStackTrace();
            bool = false;
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (session != null) {
                    session.disconnect();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return bool;
        }
    }

    /**
     * 密码方式登录 上传指定目录文件
     *
     * @param ip    host
     * @param user  用户名
     * @param pwd   密码
     * @param port  端口号
     * @param sPath 上传的sftp目录
     * @param file  要上传的文件
     * @return
     */
    public static boolean sshSftpUpLoadFile(String ip, String user, String pwd, int port, String sPath, File file) {
        try {
            Session session = getSftpSession(ip, user, pwd, port);
            upLoadFile(session, file, sPath);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean sshSftpUpLoadFile(String ip, String user, String pwd, int port, String sPath, String fileStr) {
        boolean bool = sshSftpUpLoadFile(ip, user, pwd, port, sPath, new File(fileStr));
        return bool;
    }

    /**
     * @param session
     * @param sPath   sftp的目录
     * @return 该目录下的所有文件的InputStream
     */
    public static Map<String, InputStream> readPath(Session session, String sPath) {

        Channel channel;
        Map<String, InputStream> stringHashMap = new HashMap<>();
        try {
            channel = (Channel) session.openChannel("sftp");
            channel.connect(10000000);
            ChannelSftp sftp = (ChannelSftp) channel;
            try {
                sftp.cd(sPath);
            } catch (SftpException e) {
                sftp.mkdir(sPath);
                sftp.cd(sPath);
            }
            Vector<ChannelSftp.LsEntry> listFiles = sftp.ls(sftp.pwd());
            for (ChannelSftp.LsEntry file : listFiles) {
                String fileName = file.getFilename();
                try {
                    InputStream inputStream = sftp.get(sftp.pwd() + "/" + fileName);
                    stringHashMap.put(fileName, inputStream);
                } catch (SftpException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stringHashMap;
    }

    /**
     * @param session
     * @param file
     * @return
     */
    public static InputStream readFile(Session session, String file) throws JSchException, SftpException {
        Channel channel;
        InputStream inputStream = null;
        channel = session.openChannel("sftp");
        channel.connect(10000000);
        ChannelSftp sftp = (ChannelSftp) channel;
        inputStream = sftp.get(file);
        return inputStream;
    }

    public static void upLoadFile(Session session, File file, String sPath) throws Exception {
        Channel channel = null;
        try {
            channel = (Channel) session.openChannel("sftp");
            channel.connect(10000000);
            ChannelSftp sftp = (ChannelSftp) channel;
            try {
                sftp.cd(sPath);
            } catch (SftpException e) {
                sftp.mkdir(sPath);
                sftp.cd(sPath);
            }
            copyFile(sftp, file, sftp.pwd());
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("upload file " + file + " failure");
        } finally {
            if (session != null) {
                session.disconnect();
            }
            if (channel != null) {
                channel.disconnect();
            }
        }
    }

    public static void copyFile(ChannelSftp sftp, File file, String pwd) throws SftpException, IOException {

        if (file.isDirectory()) {
            File[] list = file.listFiles();
            String fileName = file.getName();
            sftp.cd(pwd);
            sftp.mkdir(fileName);
            pwd = pwd + "/" + file.getName();
            sftp.cd(file.getName());

            for (int i = 0; i < list.length; i++) {
                copyFile(sftp, list[i], pwd);
            }
        } else {
            sftp.cd(pwd);
            InputStream instream = null;
            OutputStream outstream = null;
            try {
                if (file.exists()) {
                    outstream = sftp.put(file.getName());
                    instream = new FileInputStream(file);

                    byte b[] = new byte[1024];
                    int n;
                    try {
                        while ((n = instream.read(b)) != -1) {
                            outstream.write(b, 0, n);
                            outstream.flush();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    throw new IOException("file not exists");
                }

            } catch (IOException e) {
                throw new IOException("IOException ");
            } finally {
                try {
                    if (outstream != null) {
                        outstream.close();
                    }
                    if (instream != null) {
                        instream.close();
                    }
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
    }

    public static boolean isExistsDoneFile(String ip, String user, String pwd, int port, String file) {
        Session session = null;
        InputStream inputStream = null;
        boolean bool = false;
        try {
            session = getSftpSession(ip, user, pwd, port);
            inputStream = readFile(session, file);
            bool = true;
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (SftpException e) {
            e.printStackTrace();
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (session != null) {
                    session.disconnect();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return bool;
        }
    }
}
