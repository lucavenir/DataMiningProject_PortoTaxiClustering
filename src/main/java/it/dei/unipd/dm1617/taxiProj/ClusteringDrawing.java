package it.dei.unipd.dm1617.taxiProj;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import javax.imageio.ImageIO;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

/**
 * 
 * @author carlo
 *
 *	Classe per ottenere delle rappresentazioni grafiche del clustering.
 *	
 */
public class ClusteringDrawing
{
	BufferedImage img;
	double alfa = 0.5;

	double topLeftLatitude=Double.NaN;
	double topLeftLongitude=Double.NaN;
	double bottomRightLatitude=Double.NaN;
	double bottomRightLongitude=Double.NaN;
	
	/**
	 * Prepara un'immagine nera della dimensione specificata su cui disegnare
	 * @param width
	 * @param height
	 */
	public ClusteringDrawing(int width, int height)
	{
		img = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB_PRE);
		Graphics2D graphics = img.createGraphics();
		graphics.setPaint (Color.black );
		graphics.fillRect (0, 0, img.getWidth(), img.getHeight());
	}
	
	/**
	 * Imposta le coordinate corrispondenti ai limiti dell'immagine
	 * @param topLeftLatitude
	 * @param topLeftLongitude
	 * @param bottomRightLatitude
	 * @param bottomRightLongitude
	 * @return
	 */
	public ClusteringDrawing setLimits(double topLeftLatitude, double topLeftLongitude,	double bottomRightLatitude, double bottomRightLongitude)
	{
		this.topLeftLatitude = topLeftLatitude;
		this.topLeftLongitude = topLeftLongitude;
		this.bottomRightLatitude = bottomRightLatitude;
		this.bottomRightLongitude = bottomRightLongitude;
		return this;
	}

	/**
	 * Disegna il set di punti fornito in bianco
	 * @param pos I punti
	 * @return this
	 */
	public ClusteringDrawing draw(JavaRDD<Position> pos)
	{
		return draw(pos,null);
	}
	
	/**
	 * Disegna i punti forniti, ogni cluster sarà disegnato con un colore diverso
	 * @param pos Il set dei punti da disegnare
	 * @param kmm Il modello KMeans che specifica i cluster
	 * @return this
	 */
	public ClusteringDrawing draw(JavaRDD<Position> pos, KMeansModel kmm)
	{
		return draw(pos,kmm, null);
	}

	/**
	 * Disegna i punti forniti, ogni cluster sarà disegnato con un colore diverso
	 * @param pos Il set dei punti da disegnare
	 * @param kmm Il modello KMeans che specifica i cluster
	 * @param retDropCount Se uguale a null o con length==0 viene ignorato. Se no il numero di punti scartati viene scritto nella prima cella
	 * @return this
	 */
	public ClusteringDrawing draw(JavaRDD<Position> pos, KMeansModel kmm, int[] retDropCount)
	{
		return draw(pos,kmm, null, false);
	}
	
	
	/**
	 * Disegna i punti forniti, ogni cluster sarà disegnato con un colore diverso
	 * @param pos Il set dei punti da disegnare
	 * @param kmm Il modello KMeans che specifica i cluster
	 * @param retDropCount Se uguale a null o con length==0 viene ignorato. Se no il numero di punti scartati viene scritto nella prima cella
	 * @param writeInfoAboutTheDroppedPoints Se impostato a vero vengono scritte su standard output delle informazioni riguardo ai punti scartati
	 * @return this
	 */
	public ClusteringDrawing draw(JavaRDD<Position> pos, KMeansModel kmm, int[] retDropCount, boolean writeInfoAboutTheDroppedPoints)
	{

		JavaRDD<Tuple2<Integer, Position>> posWithClusters;
		int clustersNum = 0;
		if(kmm!=null)
		{
			posWithClusters = pos.map((p)->{
				return new Tuple2<>(kmm.predict(p.toVector()), p);
			});
			clustersNum = kmm.clusterCenters().length;
		}
		else
		{
			posWithClusters = pos.map((p)->{
				return new Tuple2<>(0, p);
			});
		}
		draw(posWithClusters, clustersNum, retDropCount, writeInfoAboutTheDroppedPoints);
		return this;
	}
	
	
	/**
	 * Disegna i punti forniti, ogni cluster sarà disegnato con un colore diverso
	 * @param pos Il set dei punti da disegnare, con l'id del cluster come chiave
	 * @param clustersNum The number of clusters
	 * @param retDropCount Se uguale a null o con length==0 viene ignorato. Se no il numero di punti scartati viene scritto nella prima cella
	 * @param writeInfoAboutTheDroppedPoints Se impostato a vero vengono scritte su standard output delle informazioni riguardo ai punti scartati
	 * @return this
	 */
	public ClusteringDrawing draw(JavaRDD<Tuple2<Integer,Position>> pos, int clustersNum, int[] retDropCount, boolean writeInfoAboutTheDroppedPoints)
	{
		//si sarebbe potuto calcolare clustersNum, ma sarebbe un po' uno spreco, il chiamante dovrebbe saperlo
		if(		Double.isNaN(topLeftLatitude) ||
				Double.isNaN(topLeftLongitude) ||
				Double.isNaN(bottomRightLatitude) ||
				Double.isNaN(bottomRightLongitude))
			throw new IllegalStateException("setLimits() must be called before calling draw()");
		
		int imgWidth = img.getWidth();
		int imgHeight = img.getHeight();
		int dropCount=0;	

		//per scegliere i colori immagino uno spazio 3D in cui ad ogni asse corrisponde un colore tra Red Green e Blue
		//In questo spazio i colori effettivi stanno in un cubo unitario.
		//A questo punto faccio l'idea sarebbe prendere una griglia di punti con passo colorCubeLatus e poi uare questi
		//punti come colori. Come colorCubeLatus prendo l'intero appena superiore alla radice cubica del numero di clusters
		int colorCubeLatus = 0;
		if((int)Math.cbrt(clustersNum+1)==Math.cbrt(clustersNum+1))
			colorCubeLatus = (int)Math.cbrt(clustersNum+1);
		else
			colorCubeLatus = (int)Math.cbrt(clustersNum+1)+1;
		int step = (int)(colorCubeLatus*colorCubeLatus*colorCubeLatus/(clustersNum+1));//siccome così il numero di punti può anche essere piuttosto più grande del numero di cluster uso solo alcuni punti, in modo da non prendere solo i primi
		
		List<Tuple2<Integer, Position>> pa = pos.collect();//bruttino, bruttino
		for (Tuple2<Integer, Position> t : pa)
		{
			int clNum = t._1()+1;
			Position p = t._2();
			
			int colorPos = clNum*step;
			
			double a = alfa;
			int r,g,b;
			if(colorCubeLatus==1)//succede se non c'è il KMeansModel
			{
				r = 0xff;
				g = 0xff;
				b = 0xff;
			}
			else
			{
				r = (int)(((((double)colorPos)									%colorCubeLatus)/colorCubeLatus)*0xff);
				g = (int)(((((double)colorPos/colorCubeLatus)					%colorCubeLatus)/colorCubeLatus)*0xff);
				b = (int)(((((double)colorPos/colorCubeLatus/colorCubeLatus)	%colorCubeLatus)/colorCubeLatus)*0xff);
			}
			
			int x = (int)Math.round((p.getPickupLongitude() - topLeftLongitude)/(bottomRightLongitude - topLeftLongitude)*imgWidth); 
			int y = (int)Math.round((topLeftLatitude - p.getPickupLatitude())/(topLeftLatitude - bottomRightLatitude)*imgHeight);
			
			int c0;
			try
			{
				c0 = img.getRGB(x, y);
			}
			catch(ArrayIndexOutOfBoundsException e)
			{
				dropCount++;
				if(writeInfoAboutTheDroppedPoints)
				{
					System.out.println(""+p.getPickupLongitude()+", "+p.getPickupLatitude()+" clNum="+clNum);
					String s = " ";
					if(p.getPickupLongitude()<topLeftLongitude)
						s += "longitude too low ";
					if(p.getPickupLongitude()>bottomRightLongitude)
						s += "longitude too high ";
					if(p.getPickupLatitude()>topLeftLatitude)
						s += "latitude too high ";
					if(p.getPickupLatitude()<bottomRightLatitude)
						s += "latitude too low ";
					System.out.println("position ("+p.getPickupLongitude()+"; "+p.getPickupLatitude()+")\t out of image area. "+
						s+
						"\t dropped "+dropCount+" of "+pa.size()+" ("+(((double)dropCount)/pa.size()*100)+"%)");
				}
				continue;
			}
			
			int c = 0xff<<24 |
					( (int)( ((c0 & 0xff0000)>>16) * (1-a) + r*a ) )<<16 |
					( (int)( ((c0 & 0xff00)  >>8 ) * (1-a) + g*a ) )<<8  |
					( (int)( ((c0 & 0xff)        ) * (1-a) + b*a ) ) ;						
			img.setRGB(x, y, c);
		}
		System.out.println("ClusteringDrawing: "+dropCount+" points were out of the image range, I've dropped them");
		if(retDropCount!=null && retDropCount.length>0)
			retDropCount[0]=dropCount;
		return this;
	}
	
	
	/**
	 * Salva l'immagine come png al percorso specificato
	 * @param path
	 * @throws IOException
	 */
	public void save(String path) throws IOException
	{
		File outputfile = new File(path);
		outputfile.getParentFile().mkdirs();
       	ImageIO.write(img, "png", outputfile);
	}

	/**
	 * Imposta la trasparenza dei punti rappresentanti le singole corse
	 * @param v 1 significa completamente opaco, 0 completamente trasparente
	 * @return this
	 */
	public ClusteringDrawing setAlfa(double v)
	{
		alfa=v;
		return this;
	}
	
	/**
	 * Disegna i centri dei cluster come dei quadrati del colore e della dimensione specificata
	 * @param r rosso: valore da 0 a 1
	 * @param g verde: valore da 0 a 1
	 * @param b blu: valore da 0 a 1
	 * @param a trasparenza: valore da 0 (trasparente) a 1 (opaco)
	 * @param centers Lista dei centri
	 * @param size Lato dei quadrati
	 * @return this
	 */
	public ClusteringDrawing drawCenters(double r, double g, double b, double a, Vector[] centers, int size)
	{
		int color = ((int)(a*0xff)) <<24 | ((int)(r*0xff))<<16 | ((int)(g*0xff))<<8 | ((int)(b*0xff));
		
		for(Vector c: centers)
		{			
			Position cp = new Position(c);
			//System.out.println(""+ca[0]+", "+ca[1]+" "+ca.length);
			int xc = (int)Math.round((cp.getPickupLongitude() - topLeftLongitude)/(bottomRightLongitude - topLeftLongitude)*img.getWidth()); 
			int yc = (int)Math.round((topLeftLatitude - cp.getPickupLatitude())/(topLeftLatitude - bottomRightLatitude)*img.getHeight());
			
			boolean pixelDropped=false;
			boolean somethingWasDrawn=false;
			for(int x=xc-size/2; x<xc+size/2;x++)
			{
				for(int y=yc-size/2; y<yc+size/2;y++)
				{
					try
					{
						img.setRGB(x, y, color);
						somethingWasDrawn=true;
					}
					catch(ArrayIndexOutOfBoundsException e)
					{
						pixelDropped = true;
					}
				}
			}
			if(pixelDropped && somethingWasDrawn)
				System.out.println("WARNING: center at ("+cp.getPickupLatitude()+" ; \t"+cp.getPickupLongitude()+") didn't completely fit (it is on the edge of the image)");
			else if(pixelDropped)
				System.out.println("WARNING: center at ("+cp.getPickupLatitude()+" ; \t"+cp.getPickupLongitude()+") was dropped, out of image area!");
		}
		return this;
	}
	
	/**
	 * Disegna i centri dei cluster come dei quadrati del colore e della dimensione specificata
	 * @param r rosso: valore da 0 a 1
	 * @param g verde: valore da 0 a 1
	 * @param b blu: valore da 0 a 1
	 * @param a trasparenza: valore da 0 (trasparente) a 1 (opaco)
	 * @param centers Lista dei centri
	 * @param size Lato dei quadrati
	 * @return this
	 */
	public ClusteringDrawing drawCenters(double r, double g, double b, double a, Position[] centers, int size)
	{
		Vector[] centersVec = new Vector[centers.length];
		for(int i=0;i<centers.length;i++)
			centersVec[i] = centers[i].toVector();
		return drawCenters(r,g,b,a,centersVec,size);
	}
}
