/**
 * Pr&aacute;ctricas de Sistemas Inform&aacute;ticos II
 * VisaCancelacionJMSBean.java
 */

package ssii2.visa;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.ejb.EJBException;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.ActivationConfigProperty;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.annotation.Resource;
import java.util.logging.Logger;

/**
 * @author jaime
 */
@MessageDriven(mappedName = "jms/VisaPagosQueue")
public class VisaCancelacionJMSBean extends DBTester implements MessageListener {
    static final Logger logger = Logger.getLogger("VisaCancelacionJMSBean");
    @Resource
    private MessageDrivenContext mdc;

    //private static final String UPDATE_CANCELA_QRY = null;
    // TODO : Definir UPDATE sobre la tabla pagos para poner
    // codRespuesta a 999 dado un código de autorización
    private static final String UPDATE_CANCELA_QRY =
        "update pago " +
        "set codRespuesta=999 " +
        "where idAutorizacion=?";
    private static final String SELECT_CODRESPUESTA_QRY =
        "select codRespuesta " +
        "from pago "+
        "where idAutorizacion=?";
    private static final String UPDATE_TARJETA_QRY =
        "update tarjeta " +
        "set saldo=saldo+pago.importe " +
        "from pago " +
        "where pago.idAutorizacion=? and pago.numeroTarjeta=tarjeta.numeroTarjeta";


    public VisaCancelacionJMSBean() {
    }

    // TODO : Método onMessage de ejemplo
    // Modificarlo para ejecutar el UPDATE definido más arriba,
    // asignando el idAutorizacion a lo recibido por el mensaje
    // Para ello conecte a la BD, prepareStatement() y ejecute correctamente
    // la actualización
    public void onMessage(Message inMessage) {
        TextMessage msg = null;
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int idAutorizacion = 0;

        try {
            if (inMessage instanceof TextMessage) {
                msg = (TextMessage) inMessage;
                logger.info("MESSAGE BEAN: Message received: " + msg.getText());

                idAutorizacion = Integer.parseInt(msg.getText());
                con = getConnection();
                pstmt = con.prepareStatement(SELECT_CODRESPUESTA_QRY);

                pstmt.setInt(1, idAutorizacion);
                rs = pstmt.executeQuery();

                if (!rs.next()) {
                    logger.info("MESSAGE BEAN: Identifier not found");
                    throw new EJBException("Identifier not found");
                }
                String codRespuesta = rs.getString("codRespuesta");
                rs.close();
                rs = null;
                pstmt.close();
                if (codRespuesta.equals("000")) {
                    // Cancela pago
                    pstmt = con.prepareStatement(UPDATE_CANCELA_QRY);
                    pstmt.setInt(1, idAutorizacion);

                    if (pstmt.execute() || pstmt.getUpdateCount() != 1) {
                        throw new EJBException();
                    }
                    pstmt.close();
                    pstmt = null;

                    // Rectifica saldo
                    pstmt = con.prepareStatement(UPDATE_TARJETA_QRY);
                    pstmt.setInt(1, idAutorizacion);

                    if (pstmt.execute() || pstmt.getUpdateCount() != 1) {
                        throw new EJBException();
                    }
                    pstmt.close();
                    pstmt = null;
                }
            } else {
                logger.warning(
                        "Message of wrong type: "
                        + inMessage.getClass().getName());
            }
        } catch (JMSException e) {
            e.printStackTrace();
            mdc.setRollbackOnly();
        } catch (Throwable te) {
            te.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close(); rs = null;
                }
                if (pstmt != null) {
                    pstmt.close(); pstmt = null;
                }
                if (con != null) {
                    closeConnection(con); con = null;
                }
            } catch (SQLException e) {
            }
        }
    }


}
